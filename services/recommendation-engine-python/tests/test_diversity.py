from collections import Counter

from app.config import DiversityConfig
from app.recommender.diversity import rerank
from app.recommender.types import Features, ScoredItem

from .factories import content

F0 = Features(0, 0, 0, 0, 0, 0, 0, 0)
FX = Features(0, 0, 0, 0, 0, 0, 0.8, 0)


def item(cid, author, category="games", score=None, tags=("rpg",), explore=False):
    return ScoredItem(content(cid, author=author, category=category, tags=tags),
                      score if score is not None else 1.0 - cid / 1000, FX if explore else F0,
                      ("explore",) if explore else ("category",))


def test_same_author_is_spread_out():
    ranked = [item(i, author=1) for i in range(1, 6)] + [item(i, author=i) for i in range(10, 20)]
    out = rerank(ranked, 10, DiversityConfig(author_gap=3, explore_every=0, author_max_share=1.0))
    authors = [it.content.author_id for it in out]
    for i in range(len(authors)):
        assert authors[i] not in authors[max(0, i - 2):i]


def test_author_cap_limits_concentration():
    ranked = [item(i, author=1) for i in range(1, 40)] + [item(i, author=i) for i in range(100, 140)]
    out = rerank(ranked, 40, DiversityConfig(author_max_share=0.1, explore_every=0))
    assert Counter(it.content.author_id for it in out)[1] <= 4


def test_size_is_preserved_even_when_constraints_cannot_be_met():
    ranked = [item(i, author=1, category="games") for i in range(1, 21)]
    out = rerank(ranked, 20, DiversityConfig())
    assert len(out) == 20 and len({it.content.id for it in out}) == 20


def test_category_window():
    ranked = [item(i, author=i, category="games", tags=(f"t{i}",)) for i in range(1, 10)] + \
             [item(i, author=i, category="musica", tags=(f"t{i}",)) for i in range(50, 60)]
    out = rerank(ranked, 12, DiversityConfig(category_window=4, category_max_in_window=2, explore_every=0))
    cats = [it.content.category for it in out]
    for i in range(len(cats)):
        assert cats[max(0, i - 3):i + 1].count(cats[i]) <= 2


def test_exploration_slot_is_filled_when_available():
    ranked = [item(i, author=i) for i in range(1, 30)] + [item(500, author=500, score=0.01, explore=True)]
    out = rerank(ranked, 16, DiversityConfig(explore_every=8))
    assert out[7].content.id == 500  # posição 8 (1-based) reservada à descoberta


def test_individual_scores_are_not_changed():
    ranked = [item(i, author=1 + i % 2) for i in range(1, 20)]
    out = rerank(ranked, 10, DiversityConfig())
    by_id = {it.content.id: it.score for it in ranked}
    assert all(by_id[it.content.id] == it.score for it in out)
