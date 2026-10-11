import random

import pytest

from app.config import RankingWeights
from app.recommender.ranker import linear_score, sort_key, top_k
from app.recommender.types import Features, ScoredItem

from .factories import content


def feats(**kw) -> Features:
    base = dict(interest=0, author=0, engagement=0, popularity=0, recency=0, similarity=0, exploration=0, exposure=0)
    base.update(kw)
    return Features(**base)


def test_linear_score_uses_weights_and_penalty():
    w = RankingWeights(interest=1, author=0, engagement=0, popularity=0, recency=0, similarity=0,
                       exploration=0, exposure_penalty=0.5)
    assert linear_score(feats(interest=0.8, exposure=0.4), w) == pytest.approx(0.6)


def test_weights_validation():
    w = RankingWeights.from_mapping({"interest": 0.9})
    assert w.interest == 0.9 and w.author == RankingWeights().author
    with pytest.raises(ValueError):
        RankingWeights.from_mapping({"nao_existe": 1})
    with pytest.raises(ValueError):
        RankingWeights.from_mapping({"interest": -1})


def test_tie_break_is_explicit_newer_first_then_id():
    f = feats()
    a = ScoredItem(content(5, age_h=2), 0.5, f, ())
    b = ScoredItem(content(3, age_h=1), 0.5, f, ())   # mais novo
    c = ScoredItem(content(1, age_h=2), 0.5, f, ())   # mesma idade de a, id menor
    assert [i.content.id for i in sorted([a, b, c], key=sort_key)] == [3, 1, 5]


def test_top_k_matches_full_sort_and_is_order_independent():
    rng = random.Random(7)
    items = [ScoredItem(content(i, age_h=rng.randint(1, 5)), round(rng.random(), 2), feats(), ()) for i in range(1, 500)]
    expected = sorted(items, key=sort_key)[:25]
    shuffled = items[:]
    rng.shuffle(shuffled)
    assert top_k(items, 25) == expected
    assert top_k(shuffled, 25) == expected
