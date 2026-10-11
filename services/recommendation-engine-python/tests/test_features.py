import math

import pytest

from app.recommender import features as F
from app.recommender.types import ContentStats

from .factories import NOW, content, profile


def test_all_features_are_bounded_0_1():
    p = profile(categories={"games": 10.0, "musica": 1.0}, tags={"rpg": 4.0, "fps": 1.0},
                authors={1: 7.0}, exposure={5: 100})
    c = content(5, tags=("rpg", "fps", "indie"))
    f = F.compute_features(p, c, ContentStats(views=0, engagement=500.0), trending=1e9, max_trending=1e9,
                           recent_items=[content(9, tags=("rpg",))], sources=("explore",), now=NOW,
                           prior_rate=0.3, recency_half_life_h=48, prior_strength=20, exposure_scale=3)
    for name, value in f.as_dict().items():
        assert 0.0 <= value <= 1.0, name


def test_interest_is_relative_to_strongest_interest():
    p = profile(categories={"games": 10.0, "musica": 2.0})
    assert F.interest_score(p, content(1, category="games", tags=())) == pytest.approx(1.0)
    assert F.interest_score(p, content(1, category="musica", tags=())) == pytest.approx(0.2)
    assert F.interest_score(p, content(1, category="arte", tags=())) == 0.0


def test_engagement_smoothing_does_not_reward_tiny_samples():
    prior = 0.3
    one_lucky_view = F.engagement_score(ContentStats(views=1, engagement=6.0), prior, 20)
    solid = F.engagement_score(ContentStats(views=1000, engagement=900.0), prior, 20)
    average = F.engagement_score(ContentStats(views=1000, engagement=300.0), prior, 20)
    assert average == pytest.approx(0.5, abs=1e-3)       # média global → 0.5
    assert solid > one_lucky_view                          # evidência > sorte
    assert F.engagement_score(ContentStats(), prior, 20) == pytest.approx(0.5)  # sem dados → prior


def test_raw_counts_do_not_dominate_popularity():
    # log-normalização: 10x mais eventos não dá 10x mais score
    assert F.popularity_score(1000, 10000) > 0.7
    assert F.popularity_score(0, 10000) == 0.0


def test_recency_half_life():
    assert F.recency_score(NOW, NOW, 48) == pytest.approx(1.0)
    assert F.recency_score(NOW - 48 * 3600, NOW, 48) == pytest.approx(0.5)
    assert F.recency_score(NOW + 100, NOW, 48) == pytest.approx(1.0)  # relógio adiantado não passa de 1


def test_exposure_penalty_grows_with_repeated_impressions():
    assert F.exposure_score(0, 3) == 0.0
    assert F.exposure_score(1, 3) < F.exposure_score(5, 3) < 1.0
    assert F.exposure_score(3, 3) == pytest.approx(1 - math.exp(-1))


def test_exploration_is_deterministic_and_only_for_unknown_authors():
    p = profile(authors={1: 5.0})
    known = content(10, author=1)
    unknown = content(11, author=2)
    s = ContentStats(views=5)
    assert F.exploration_score(p, known, s, ("category",), NOW) == 0.0
    a = F.exploration_score(p, unknown, s, ("category",), NOW)
    b = F.exploration_score(p, unknown, s, ("category",), NOW)
    assert a == b and 0.5 <= a < 1.0
    # conteúdo velho e muito visto de autor desconhecido só entra se veio da fonte explore
    old = content(12, author=3, age_h=500)
    assert F.exploration_score(p, old, ContentStats(views=1000), ("trending",), NOW) == 0.0
    assert F.exploration_score(p, old, ContentStats(views=1000), ("explore",), NOW) > 0.0


def test_similarity_uses_recent_items():
    c = content(1, category="games", tags=("rpg", "indie"))
    assert F.similarity_score(c, []) == 0.0
    assert F.similarity_score(c, [content(2, category="musica", tags=("rpg", "indie"))]) == pytest.approx(1.0)
    assert F.similarity_score(c, [content(1, tags=("rpg", "indie"))]) == 0.0  # ignora o próprio item
