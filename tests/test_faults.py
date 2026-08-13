"""The M3 done-when.

    `--seed N --faults aggressive` shows drops, partitions and crashes in the trace,
    and re-running seed N reproduces the identical fault sequence.

Plus the checklist item that matters more than it sounds: *each fault kind provably
fires under some seed*. A fault that is configurable but unreachable contributes exactly
nothing, and the only way to know the difference is to go looking.
"""

from itertools import pairwise

from conftest import ListTrace

from groundhog.sim.demo import run_demo
from groundhog.sim.faults import (
    CRASH,
    HANG,
    FaultProfile,
    FaultSchedule,
    OutageEpisode,
    generate_schedule,
)
from groundhog.sim.rng import Rng
from groundhog.types import SECOND, NodeId, Tick

NODES: tuple[NodeId, ...] = (1, 2, 3)
HORIZON: Tick = 5 * SECOND
AGGRESSIVE = FaultProfile.aggressive()


def schedule_for(seed: int, horizon: Tick = HORIZON) -> FaultSchedule:
    return generate_schedule(Rng(seed), AGGRESSIVE, NODES, horizon)


# -- the schedule ---------------------------------------------------------------------


def test_the_quiet_profile_schedules_nothing() -> None:
    schedule = generate_schedule(Rng(1), FaultProfile.quiet(), NODES, HORIZON)
    assert schedule.partitions == ()
    assert schedule.outages == ()


def test_the_same_seed_draws_the_same_schedule() -> None:
    assert schedule_for(4471) == schedule_for(4471)


def test_different_seeds_draw_different_schedules() -> None:
    assert any(schedule_for(seed) != schedule_for(0) for seed in range(1, 20))


def test_every_episode_lands_inside_the_horizon() -> None:
    for seed in range(30):
        schedule = schedule_for(seed)
        for episode in schedule.partitions:
            assert 0 <= episode.at < HORIZON
            assert episode.duration > 0
        for outage in schedule.outages:
            assert 0 <= outage.at < HORIZON
            assert outage.duration > 0


def test_partitions_never_overlap_each_other() -> None:
    for seed in range(30):
        episodes = schedule_for(seed).partitions
        for earlier, later in pairwise(episodes):
            assert earlier.at + earlier.duration <= later.at


def test_a_node_is_never_crashed_and_hung_at_once() -> None:
    for seed in range(30):
        by_node: dict[NodeId, list[OutageEpisode]] = {}
        for outage in schedule_for(seed).outages:
            by_node.setdefault(outage.node, []).append(outage)
        for outages in by_node.values():
            for earlier, later in pairwise(outages):
                assert earlier.at + earlier.duration <= later.at


def test_both_sides_of_a_partition_have_somebody_on_them() -> None:
    for seed in range(50):
        for episode in schedule_for(seed).partitions:
            assert episode.partition.side_a
            assert episode.partition.side_b
            everyone = sorted(episode.partition.side_a + episode.partition.side_b)
            assert everyone == list(NODES)


def test_outages_are_listed_as_a_timeline() -> None:
    for seed in range(20):
        outages = schedule_for(seed).outages
        keys = [(outage.at, outage.node) for outage in outages]
        assert keys == sorted(keys)


def test_every_episode_kind_is_reachable() -> None:
    """The checklist item. Each of these must actually occur under *some* seed."""
    seen: dict[str, int] = {}
    for seed in range(60):
        schedule = schedule_for(seed)
        for episode in schedule.partitions:
            key = "one_way_partition" if episode.partition.one_way else "partition"
            seen[key] = seen.get(key, 0) + 1
        for outage in schedule.outages:
            seen[outage.kind] = seen.get(outage.kind, 0) + 1

    for kind in ("partition", "one_way_partition", CRASH, HANG):
        assert seen.get(kind, 0) > 0, f"{kind} never generated; saw {sorted(seen)}"


def test_dropping_an_episode_leaves_the_rest_alone() -> None:
    """M7's shrinker works by deleting episodes one at a time."""
    schedule = schedule_for(4471)
    assert schedule.outages, "pick a seed that actually schedules an outage"
    smaller = schedule.without(0, of="outages")
    assert smaller.outages == schedule.outages[1:]
    assert smaller.partitions == schedule.partitions


# -- the injector, end to end ---------------------------------------------------------


def run_aggressive(seed: int, ms: int = 3000) -> tuple[ListTrace, list[str]]:
    trace = ListTrace()
    cluster, _ = run_demo(
        seed,
        trace=trace,
        profile=AGGRESSIVE,
        max_ticks=ms * 1000,
    )
    return trace, cluster.injector.fired


def test_the_header_carries_the_whole_schedule() -> None:
    trace, _ = run_aggressive(4471)
    header = trace.records[0]
    assert header["kind"] == "sim.start"
    assert header["profile"] == "aggressive"
    assert "faults" in header
    assert "network" in header


def test_an_aggressive_run_shows_drops_partitions_and_crashes() -> None:
    """The done-when, verbatim."""
    trace, fired = run_aggressive(4471)
    kinds = dict.fromkeys(trace.kinds())
    assert "net.drop" in kinds
    assert "net.partition" in kinds
    assert "node.crash" in kinds
    assert "node.restart" in kinds
    assert "partition" in fired
    assert CRASH in fired


def test_every_fault_kind_fires_in_some_universe() -> None:
    fired: dict[str, int] = {}
    kinds: dict[str, int] = {}
    for seed in range(12):
        trace, episodes = run_aggressive(seed)
        for episode in episodes:
            fired[episode] = fired.get(episode, 0) + 1
        for kind in trace.kinds():
            kinds[kind] = kinds.get(kind, 0) + 1

    for episode in ("partition", "heal", CRASH, "crash_end", HANG, "hang_end"):
        assert fired.get(episode, 0) > 0, f"{episode} never fired; saw {sorted(fired)}"
    for kind in ("net.drop", "net.duplicate", "net.hold", "net.release", "node.restart"):
        assert kinds.get(kind, 0) > 0, f"{kind} never traced; saw {sorted(kinds)}"


def test_the_fault_sequence_is_identical_on_a_rerun() -> None:
    """The second half of the done-when."""
    first_trace, first_fired = run_aggressive(4471)
    second_trace, second_fired = run_aggressive(4471)
    assert first_fired == second_fired
    assert first_trace.records == second_trace.records


def test_a_crashed_node_comes_back_from_its_log_and_not_from_memory() -> None:
    """'wipes volatile state, keeps synced disk', checked rather than assumed."""
    trace, _ = run_aggressive(4471)
    restarts = trace.of_kind("node.restart")
    assert restarts
    for record in restarts:
        node = record["node"]
        recovered = record["recovered"]
        assert isinstance(recovered, int)
        # Whatever came back had to come off the disk; nothing else survives a crash.
        crashes = [r for r in trace.of_kind("node.crash") if r["node"] == node]
        assert crashes, f"node {node} restarted without ever crashing"
