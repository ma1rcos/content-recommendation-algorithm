"""Gerador de dados sintéticos reproduzíveis.

    python -m app.seed --reset [--users 2000 --authors 300 --contents 20000 --seed 42]

Com o mesmo --seed e o mesmo --reference-time, o conteúdo gerado é idêntico.
Usuários de demonstração com IDs fixos:
  1 ana_games    — gosta de games e tecnologia (histórico rico)
  2 bruno_chef   — gosta de culinária e viagem (histórico rico)
  3 carla_nova   — nova, declarou interesse em música, sem interações
  4 davi_zero    — nova, sem interesses declarados nem interações (cold start puro)
Depois de gravar no PostgreSQL, limpa as chaves `rec:*` e reconstrói o catálogo
no Redis — o mesmo caminho usado na recuperação após perda do Redis.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

from .config import load_settings
from .runtime import make_pool, make_redis
from .state_rebuild import rebuild_catalog

TAXONOMY = Path(os.environ.get("TAXONOMY_FILE") or
                Path(__file__).resolve().parents[3] / "database" / "seeds" / "taxonomy.json")
SHOWCASE = [
    (1, "ana_games", ["games", "tecnologia"], True),
    (2, "bruno_chef", ["culinaria", "viagem"], True),
    (3, "carla_nova", ["musica"], False),
    (4, "davi_zero", [], False),
]
EXPLICIT_PROBS = (("content_liked", 0.30), ("content_saved", 0.08),
                  ("content_commented", 0.05), ("content_shared", 0.04))


def parse_args() -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser()
    p.add_argument("--users", type=int, default=int(env("SEED_USERS", "2000")))
    p.add_argument("--authors", type=int, default=int(env("SEED_AUTHORS", "300")))
    p.add_argument("--contents", type=int, default=int(env("SEED_CONTENTS", "20000")))
    p.add_argument("--days", type=int, default=30, help="janela de publicação dos conteúdos")
    p.add_argument("--seed", type=int, default=int(env("SEED_RANDOM", "42")))
    p.add_argument("--reference-time", default=None, help="ISO-8601 UTC; padrão: agora")
    p.add_argument("--reset", action="store_true", help="apaga dados existentes antes de gerar")
    return p.parse_args()


def generate(args: argparse.Namespace, now: datetime) -> dict[str, list[tuple]]:
    rng = random.Random(args.seed)
    tax = json.loads(TAXONOMY.read_text(encoding="utf-8"))
    cats = tax["categories"]
    cat_ids = {c["slug"]: i + 1 for i, c in enumerate(cats)}
    tag_slugs = sorted({t for c in cats for t in c["tags"]} | set(tax["generic_tags"]))
    tag_ids = {t: i + 1 for i, t in enumerate(tag_slugs)}
    words = tax["title_words"]

    data: dict[str, list[tuple]] = {k: [] for k in (
        "categories", "tags", "users", "declared", "authors", "blocks", "contents", "content_tags", "interactions")}
    data["categories"] = [(cat_ids[c["slug"]], c["slug"], c["name"]) for c in cats]
    data["tags"] = [(tag_ids[t], t) for t in tag_slugs]

    # Usuários: cada um tem 1–3 categorias favoritas (personas).
    user_favs: dict[int, list[str]] = {}
    active_users: set[int] = set()
    for uid, handle, favs, active in SHOWCASE:
        data["users"].append((uid, handle, now - timedelta(days=60)))
        user_favs[uid] = favs
        for f in favs:
            data["declared"].append((uid, cat_ids[f]))
        if active:
            active_users.add(uid)
    slugs = list(cat_ids)
    for uid in range(len(SHOWCASE) + 1, args.users + 1):
        favs = rng.sample(slugs, rng.choice((1, 2, 2, 3)))
        user_favs[uid] = favs
        data["users"].append((uid, f"user_{uid}", now - timedelta(days=rng.randint(1, 365))))
        if rng.random() < 0.6:
            for f in favs[:2]:
                data["declared"].append((uid, cat_ids[f]))
        if rng.random() < 0.9:  # 10% são usuários novos, sem histórico
            active_users.add(uid)

    # Autores: popularidade com cauda longa (Zipf) e 1–2 categorias principais.
    author_cats: dict[int, list[str]] = {}
    author_pop: dict[int, float] = {}
    for aid in range(1, args.authors + 1):
        author_cats[aid] = rng.sample(slugs, rng.choice((1, 1, 2)))
        author_pop[aid] = 1.0 / (rng.randint(1, args.authors) ** 0.8)
        data["authors"].append((aid, f"Autor {aid:04d}", now - timedelta(days=400)))
    by_cat_authors = {c: [a for a in author_cats if c in author_cats[a]] for c in slugs}
    for c in slugs:  # garante ao menos um autor por categoria
        if not by_cat_authors[c]:
            a = rng.randint(1, args.authors)
            author_cats[a].append(c)
            by_cat_authors[c].append(a)

    # Conteúdos: idade com viés para o recente (exponencial), tags da categoria.
    contents_by_cat: dict[str, list[tuple[int, int, float, float]]] = {c: [] for c in slugs}
    cat_tags = {c["slug"]: c["tags"] for c in cats}
    for cid in range(1, args.contents + 1):
        aid = rng.choices(range(1, args.authors + 1), weights=[author_pop[a] for a in range(1, args.authors + 1)])[0] \
            if rng.random() < 0.5 else rng.randint(1, args.authors)
        cat = rng.choice(author_cats[aid])
        age_h = min(args.days * 24.0, rng.expovariate(1 / (args.days * 24 / 4)))
        published = now - timedelta(hours=age_h)
        status = "removed" if rng.random() < 0.01 else "active"
        title = f"{words['nouns'][rng.randrange(10)].capitalize()} {words['adjectives'][rng.randrange(10)]} de {cat} #{cid}"
        data["contents"].append((cid, aid, cat_ids[cat], title, status, published, published))
        tags = rng.sample(cat_tags[cat], rng.randint(1, 3))
        if rng.random() < 0.4:
            tags.append(rng.choice(tax["generic_tags"]))
        for t in tags:
            data["content_tags"].append((cid, tag_ids[t]))
        quality = rng.betavariate(2, 5)  # alguns conteúdos engajam mais que outros
        contents_by_cat[cat].append((cid, aid, age_h, quality))

    cat_weights = {c: [author_pop[a] * (0.3 + q) * math.exp(-age / 240) for _, a, age, q in contents_by_cat[c]]
                   for c in slugs}

    # Bloqueios: ~2% dos usuários bloqueiam um autor.
    for uid in range(1, args.users + 1):
        if uid > len(SHOWCASE) and rng.random() < 0.02:
            data["blocks"].append((uid, rng.randint(1, args.authors), now))

    # Interações: 85% nas categorias favoritas; views sempre, explícitas com prob.
    for uid in sorted(active_users):
        n = 80 if uid <= 2 else rng.randint(10, 70)
        favs = user_favs[uid]
        seen_c: set[int] = set()
        for _ in range(n):
            cat = rng.choice(favs) if rng.random() < 0.85 or uid <= 2 else rng.choice(slugs)
            pool = contents_by_cat[cat]
            if not pool:
                continue
            cid, _aid, age_h, quality = rng.choices(pool, weights=cat_weights[cat])[0]
            if cid in seen_c:
                continue
            seen_c.add(cid)
            # instante da interação: entre a publicação e agora, enviesado para o recente
            delay_h = rng.uniform(0, age_h) ** 0.7 if age_h > 0 else 0.0
            at = now - timedelta(hours=max(0.0, age_h - delay_h))
            evid = lambda: str(uuid.UUID(int=rng.getrandbits(128), version=4))  # noqa: E731
            data["interactions"].append((evid(), uid, cid, "content_viewed", at))
            for etype, p in EXPLICIT_PROBS:
                if rng.random() < p * (0.6 + quality):
                    data["interactions"].append((evid(), uid, cid, etype, at + timedelta(seconds=rng.randint(5, 600))))
    # nenhuma interação "no futuro" em relação à referência
    data["interactions"] = [row if row[4] <= now else (*row[:4], now) for row in data["interactions"]]
    return data


COPY_SPECS = [
    ("categories", "categories (id, slug, name)"),
    ("tags", "tags (id, slug)"),
    ("users", "users (id, handle, created_at)"),
    ("declared", "user_declared_interests (user_id, category_id)"),
    ("authors", "authors (id, name, created_at)"),
    ("blocks", "user_blocks (user_id, author_id, created_at)"),
    ("contents", "contents (id, author_id, category_id, title, status, published_at, created_at)"),
    ("content_tags", "content_tags (content_id, tag_id)"),
    ("interactions", "interactions (event_id, user_id, content_id, type, occurred_at)"),
]


def write_db(dsn: str, data: dict[str, list[tuple]], reset: bool) -> None:
    with psycopg.connect(dsn) as conn:
        if reset:
            conn.execute("TRUNCATE interactions, content_tags, contents, user_blocks, authors, "
                         "user_declared_interests, users, tags, categories RESTART IDENTITY CASCADE")
        elif conn.execute("SELECT EXISTS(SELECT 1 FROM users)").fetchone()[0]:
            raise SystemExit("o banco já tem dados; use --reset para recriar")
        for key, target in COPY_SPECS:
            with conn.cursor().copy(f"COPY {target} FROM STDIN") as cp:
                for row in data[key]:
                    cp.write_row(row)
        for table in ("users", "authors", "contents"):
            conn.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                         f"(SELECT COALESCE(max(id), 1) FROM {table}))")
        conn.commit()


def reset_redis(r) -> int:
    n = 0
    for key in r.scan_iter(match="rec:*", count=1000):
        r.unlink(key)
        n += 1
    return n


def main() -> None:
    args = parse_args()
    now = (datetime.fromisoformat(args.reference_time).astimezone(timezone.utc)
           if args.reference_time else datetime.now(timezone.utc)).replace(microsecond=0)
    t0 = time.monotonic()
    data = generate(args, now)
    dsn = os.environ.get("MIGRATIONS_DATABASE_URL") or os.environ["DATABASE_URL"]
    write_db(dsn, data, args.reset)
    t1 = time.monotonic()
    s = load_settings()
    r, pool = make_redis(s), make_pool(s)
    pool.open()
    removed = reset_redis(r)
    counts = rebuild_catalog(r, pool, s)
    pool.close()
    summary = {k: len(v) for k, v in data.items()}
    print(json.dumps({"postgres": summary, "postgres_seconds": round(t1 - t0, 2),
                      "redis_keys_removed": removed, "redis_catalog": counts}, ensure_ascii=False))


if __name__ == "__main__":
    main()
