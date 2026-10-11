from dataclasses import replace

from app.config import RankingWeights
from app.recommender.pipeline import merge_sources, rank_feed
from app.recommender.profile import compute_profile, decay
from app.recommender.types import BuildStats

from .factories import NOW, candidates, content, profile, settings, stats_for


def catalog_two_worlds():
    games = {i: content(i, author=i % 7 + 1, category="games", tags=("rpg", "fps")[i % 2:i % 2 + 1]) for i in range(1, 61)}
    food = {i: content(i, author=i % 7 + 20, category="culinaria", tags=("receitas",)) for i in range(100, 161)}
    return {**games, **food}


def test_merge_sources_dedups_keeps_all_sources_and_caps():
    merged = merge_sources({"author": [1, 2, 3], "category": [3, 4], "trending": [5, 1]}, total_cap=4)
    assert list(merged) == [1, 3, 5, 2]  # round-robin entre fontes
    assert merged[1] == ("author", "trending") and merged[3] == ("category", "author")
    assert len(merged) == 4


def test_filters_are_mandatory_and_counted():
    cat = {1: content(1), 2: content(2, status="removed"), 3: content(3, author=9), 4: content(4), 5: content(5)}
    p = profile(seen={4}, blocked_authors={9}, interaction_count=3, categories={"games": 1.0})
    bs = BuildStats()
    out = rank_feed(p, candidates([1, 2, 3, 4, 5, 6]), cat, stats_for(cat), {}, [], 0.3, NOW, settings(), bs)
    assert {i.content_id for i in out} == {1, 5}
    assert bs.filtered == {"missing": 1, "removed": 1, "blocked_author": 1, "seen": 1}


def test_different_users_get_different_feeds():
    cat = catalog_two_worlds()
    gamer = profile(1, categories={"games": 10.0, "culinaria": 0.5}, interaction_count=20)
    cook = profile(2, categories={"culinaria": 10.0, "games": 0.5}, interaction_count=20)
    s = settings(feed_size=20)
    cands = candidates(cat)
    top_g = rank_feed(gamer, cands, cat, stats_for(cat), {}, [], 0.3, NOW, s)
    top_c = rank_feed(cook, cands, cat, stats_for(cat), {}, [], 0.3, NOW, s)
    share_g = sum(cat[i.content_id].category == "games" for i in top_g) / len(top_g)
    share_c = sum(cat[i.content_id].category == "culinaria" for i in top_c) / len(top_c)
    assert share_g >= 0.7 and share_c >= 0.7


def test_ranking_is_deterministic():
    cat = catalog_two_worlds()
    p = profile(1, categories={"games": 3.0}, interaction_count=5)
    a = rank_feed(p, candidates(cat), cat, stats_for(cat), {}, [], 0.3, NOW, settings(feed_size=30))
    b = rank_feed(p, candidates(reversed(list(cat))), cat, stats_for(cat), {}, [], 0.3, NOW, settings(feed_size=30))
    assert [i.content_id for i in a] == [i.content_id for i in b]


def test_changing_weights_changes_results():
    cat = {i: content(i, author=i, category="games" if i < 50 else "musica", age_h=1 if i >= 50 else 300)
           for i in range(1, 100)}
    p = profile(1, categories={"games": 5.0}, interaction_count=5)
    interest_heavy = settings(feed_size=10, weights=RankingWeights(interest=1, recency=0.0, author=0, engagement=0,
                                                                   popularity=0, similarity=0, exploration=0))
    recency_heavy = replace(interest_heavy, weights=RankingWeights(interest=0, recency=1, author=0, engagement=0,
                                                                   popularity=0, similarity=0, exploration=0))
    a = rank_feed(p, candidates(cat), cat, stats_for(cat), {}, [], 0.3, NOW, interest_heavy)
    b = rank_feed(p, candidates(cat), cat, stats_for(cat), {}, [], 0.3, NOW, recency_heavy)
    assert all(cat[i.content_id].category == "games" for i in a)
    assert all(cat[i.content_id].category == "musica" for i in b)


def test_cold_start_uses_cold_weights_and_still_fills_feed():
    cat = {i: content(i, author=i, age_h=i) for i in range(1, 40)}
    trending = {i: float(40 - i) for i in cat}
    bs = BuildStats()
    out = rank_feed(profile(99), candidates(cat, "trending"), cat, {}, trending, [], 0.3, NOW,
                    settings(feed_size=20), bs)
    assert bs.strategy == "cold_start" and len(out) == 20


def test_global_popularity_does_not_override_interests():
    cat = {i: content(i, author=i, category="games") for i in range(1, 31)}
    cat.update({i: content(i, author=i, category="humor") for i in range(100, 131)})
    trending = {i: 1000.0 for i in range(100, 131)}  # humor é viral
    p = profile(1, categories={"games": 10.0}, authors={1: 1.0}, interaction_count=10)
    out = rank_feed(p, candidates(cat), cat, stats_for(cat), trending, [], 0.3, NOW, settings(feed_size=10))
    assert sum(cat[i.content_id].category == "games" for i in out) >= 6


def test_profile_decay_matches_formula():
    s = settings()
    meta = content(1, author=3, category="games", tags=("rpg",))
    p = compute_profile([("content_liked", meta, NOW - s.profile_half_life_s)], ["musica"], NOW,
                        s.profile_half_life_s, s.seen_ttl_s, (50, 100, 200), 20, 500)
    assert p.categories["games"] == 3.0 * 0.5
    assert p.categories["musica"] == 1.0  # interesse declarado = base do cold start
    assert p.tags["rpg"] == 3.0 * 0.5 * 0.5
    assert p.authors[3] == 1.5 and p.interaction_count == 1
    assert decay(0, 10) == 1.0
