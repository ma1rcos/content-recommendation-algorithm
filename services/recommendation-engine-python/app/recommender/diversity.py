"""Reordenação gulosa por diversidade.

Percorre a lista já ranqueada e, para cada posição, escolhe o primeiro item
(em ordem de score) que respeita todas as restrições. Se nenhum item dentro
do lookahead respeitar, as restrições são relaxadas em ordem — similaridade,
categoria, intervalo de autor, cota de autor — para nunca encolher o feed.
O score individual NÃO muda: muda apenas a posição final.
"""

from __future__ import annotations

from collections import Counter, deque

from ..config import DiversityConfig
from .features import jaccard
from .types import ScoredItem

RELAX_LEVELS = ("all", "no_similarity", "no_category", "no_author_gap", "none")


def _ok(item: ScoredItem, out: list[ScoredItem], author_count: Counter, author_cap: int,
        cfg: DiversityConfig, level: int) -> bool:
    c = item.content
    if level < 4 and author_count[c.author_id] >= author_cap:
        return False
    if level < 3:
        for prev in out[-(cfg.author_gap - 1):] if cfg.author_gap > 1 else []:
            if prev.content.author_id == c.author_id:
                return False
    if level < 2 and cfg.category_window > 1:
        window = out[-(cfg.category_window - 1):]
        if sum(1 for p in window if p.content.category == c.category) >= cfg.category_max_in_window:
            return False
    if level < 1 and out:
        if jaccard(out[-1].content.tags, c.tags) > cfg.similarity_max:
            return False
    return True


def rerank(ranked: list[ScoredItem], size: int, cfg: DiversityConfig) -> list[ScoredItem]:
    size = min(size, len(ranked))
    if size == 0:
        return []
    author_cap = max(2, int(cfg.author_max_share * size))
    remaining: deque[ScoredItem] = deque(ranked)
    out: list[ScoredItem] = []
    author_count: Counter = Counter()

    while len(out) < size and remaining:
        position = len(out) + 1
        want_explore = cfg.explore_every > 0 and position % cfg.explore_every == 0
        chosen_idx = None
        window = min(cfg.lookahead, len(remaining))
        for level in range(len(RELAX_LEVELS)):
            if want_explore:
                for i in range(len(remaining)):
                    it = remaining[i]
                    if it.is_exploration and _ok(it, out, author_count, author_cap, cfg, level):
                        chosen_idx = i
                        break
                if chosen_idx is not None:
                    break
            for i in range(window):
                if _ok(remaining[i], out, author_count, author_cap, cfg, level):
                    chosen_idx = i
                    break
            if chosen_idx is not None:
                break
        if chosen_idx is None:  # inalcançável (nível "none" aceita tudo), mas seguro
            chosen_idx = 0
        item = remaining[chosen_idx]
        del remaining[chosen_idx]
        out.append(item)
        author_count[item.content.author_id] += 1
    return out
