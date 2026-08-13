"""The generator is the foundation. If it wobbles, nothing above it means anything."""

import pytest

from groundhog.sim.rng import Rng

SEED = 4471


def test_same_seed_gives_the_same_sequence() -> None:
    a = Rng(SEED)
    b = Rng(SEED)
    assert [a.below(1000) for _ in range(50)] == [b.below(1000) for _ in range(50)]


def test_different_seeds_diverge() -> None:
    a = [Rng(SEED).below(1_000_000) for _ in range(1)]
    b = [Rng(SEED + 1).below(1_000_000) for _ in range(1)]
    assert a != b


def test_golden_values_pin_the_mersenne_twister() -> None:
    """Literal values, on purpose.

    These are what CPython's Mersenne Twister produces for seed 4471. If an interpreter
    upgrade ever changes the stream, this fails loudly here -- rather than a sweep
    silently covering a different million universes than the one that found a bug.
    """
    assert [Rng(SEED).below(100) for _ in range(1)] == [51]
    rng = Rng(SEED)
    assert [rng.below(100) for _ in range(8)] == [51, 83, 12, 11, 80, 19, 65, 79]
    assert [Rng(SEED).between(10, 20) for _ in range(1)] == [16]


def test_below_stays_in_range() -> None:
    rng = Rng(SEED)
    for n in (1, 2, 3, 7, 8, 100, 1023, 1024):
        for _ in range(200):
            assert 0 <= rng.below(n) < n


def test_below_one_is_always_zero() -> None:
    rng = Rng(SEED)
    assert [rng.below(1) for _ in range(10)] == [0] * 10


@pytest.mark.parametrize("n", [0, -1, -100])
def test_below_rejects_non_positive(n: int) -> None:
    with pytest.raises(ValueError, match="n >= 1"):
        Rng(SEED).below(n)


def test_below_is_roughly_uniform() -> None:
    """Rejection sampling done wrong skews low. This would catch that."""
    rng = Rng(SEED)
    counts = [0, 0, 0, 0]
    for _ in range(4000):
        counts[rng.below(4)] += 1
    assert all(850 <= count <= 1150 for count in counts), counts


def test_between_is_inclusive_at_both_ends() -> None:
    rng = Rng(SEED)
    seen: dict[int, None] = {}
    for _ in range(500):
        value = rng.between(5, 7)
        assert 5 <= value <= 7
        seen[value] = None
    assert sorted(seen) == [5, 6, 7]


def test_between_allows_a_single_point() -> None:
    assert Rng(SEED).between(42, 42) == 42


def test_between_rejects_an_inverted_range() -> None:
    with pytest.raises(ValueError, match="low <= high"):
        Rng(SEED).between(10, 9)


def test_chance_at_the_extremes() -> None:
    rng = Rng(SEED)
    assert not any(rng.chance(0) for _ in range(200))
    assert all(rng.chance(100) for _ in range(200))


def test_chance_consumes_a_draw_even_when_the_answer_is_fixed() -> None:
    """Keeps the stream aligned when a fault probability is dialled to zero."""
    rng = Rng(SEED)
    rng.chance(0)
    assert rng.calls == 1


@pytest.mark.parametrize("percent", [-1, 101])
def test_chance_rejects_impossible_percentages(percent: int) -> None:
    with pytest.raises(ValueError, match="0 <= percent <= 100"):
        Rng(SEED).chance(percent)


def test_chance_is_roughly_calibrated() -> None:
    rng = Rng(SEED)
    hits = sum(1 for _ in range(4000) if rng.chance(25))
    assert 900 <= hits <= 1100, hits


def test_pick_returns_a_member() -> None:
    rng = Rng(SEED)
    items = ("a", "b", "c")
    seen: dict[str, None] = {}
    for _ in range(200):
        choice = rng.pick(items)
        assert choice in items
        seen[choice] = None
    assert sorted(seen) == ["a", "b", "c"]


def test_pick_rejects_an_empty_sequence() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        Rng(SEED).pick([])


def test_call_counter_tracks_logical_draws() -> None:
    rng = Rng(SEED)
    assert rng.calls == 0
    rng.below(10)
    rng.between(1, 5)
    rng.chance(50)
    rng.pick([1, 2, 3])
    assert rng.calls == 4


def test_repr_shows_seed_and_progress() -> None:
    rng = Rng(SEED)
    rng.below(10)
    assert repr(rng) == "Rng(seed=4471, calls=1)"
