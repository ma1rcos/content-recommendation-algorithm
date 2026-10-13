"""Compara o feed de um usuário com os pesos atuais e com pesos alternativos,
SEM gravar nada no Redis. Útil para verificar se uma mudança de pesos realmente
altera o resultado antes de aplicá-la.

    python -m app.explain --user 1 --weights '{"recency": 1.0, "interest": 0.1}' [--top 20]

Com Docker Compose:
    docker compose exec engine python -m app.explain --user 1 --weights '{"recency":1}'
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import replace

from .config import RankingWeights, load_settings
from .feed_builder import load_inputs
from .recommender.pipeline import rank_feed
from .runtime import make_pool, make_redis
from .store import now_s


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--user", type=int, required=True)
    p.add_argument("--weights", default="{}", help="JSON parcial com pesos alternativos")
    p.add_argument("--top", type=int, default=20)
    args = p.parse_args()

    s = load_settings()
    alt = replace(s, weights=RankingWeights.from_mapping(json.loads(args.weights), s.weights))
    r, pool = make_redis(s), make_pool(s, max_size=2)
    pool.open()
    try:
        inp = load_inputs(r, pool, args.user, s)
        now = now_s()
        feeds = {}
        for name, cfg in (("atual", s), ("alternativo", alt)):
            items = rank_feed(inp.profile, inp.candidates, inp.catalog, inp.stats, inp.trending,
                              inp.recent_items, inp.prior, now, cfg)[:args.top]
            feeds[name] = items
        a = {i.content_id for i in feeds["atual"]}
        b = {i.content_id for i in feeds["alternativo"]}
        print(f"usuário {args.user} | estratégia: {'cold_start' if inp.profile.is_cold else 'personalized'}")
        print(f"pesos atuais:       {s.weights}")
        print(f"pesos alternativos: {alt.weights}")
        print(f"sobreposição do top {args.top}: {len(a & b)}/{args.top} (Jaccard {len(a & b) / max(1, len(a | b)):.2f})")
        for name, items in feeds.items():
            cats = Counter(inp.catalog[i.content_id].category for i in items)
            reasons = Counter(i.reason for i in items)
            print(f"\n[{name}] categorias: {dict(cats.most_common())}")
            print(f"[{name}] motivos:    {dict(reasons.most_common())}")
            for pos, it in enumerate(items[:10]):
                c = inp.catalog[it.content_id]
                print(f"  {pos:2d}. #{it.content_id:<6} {c.category:<11} autor {c.author_id:<4} score {it.score:.4f}  {it.reason}")
    finally:
        pool.close()


if __name__ == "__main__":
    main()
