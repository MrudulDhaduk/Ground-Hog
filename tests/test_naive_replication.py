"""The tests your rung-3 replicator has to pass -- and the one it has to fail.

This whole file skips itself until you set `IMPLEMENTED = True` in
`groundhog/naive/replicator.py`, so the suite stays green while the assignment is
outstanding. Run `pytest -rs` to see the skip reasons.

Read the split carefully, because it is the point of the milestone:

- Under `perfect` -- constant latency, nothing else -- the three copies **must** agree.
  If they do not, the replicator is not merely naive, it is broken in a way Raft would
  not fix, and you should find that before blaming the network.
- Under `quiet` -- variable latency, and *nothing else at all* -- they will not always
  agree, and `test_variable_latency_alone_is_enough_to_break_it` asserts that. No drops,
  no partitions, no crashes, no disk faults. One message taking longer than another is
  the entire fault.

That second test is the milestone. Everything after it in this project exists because
of it.
"""

from collections.abc import Callable

import pytest
from conftest import ListTrace

from groundhog.kv import Command
from groundhog.naive.replicator import IMPLEMENTED
from groundhog.naive.world import PRIMARY, NaiveCluster, NaiveResult, run_naive
from groundhog.sim.faults import FaultProfile

pytestmark = pytest.mark.skipif(
    not IMPLEMENTED,
    reason="M4 [Y]: write groundhog/naive/replicator.py, then set IMPLEMENTED = True",
)

PERFECT = FaultProfile.perfect()
QUIET = FaultProfile.quiet()
AGGRESSIVE = FaultProfile.aggressive()
SEED = 4471


def run(seed: int, profile: FaultProfile, writes: int = 40) -> NaiveResult:
    return run_naive(seed, trace=ListTrace(), profile=profile, writes=writes)


def first_seed_where(
    profile: FaultProfile,
    broken: Callable[[NaiveResult], bool],
    limit: int = 200,
) -> int | None:
    """The first universe in which something went wrong. Stops at the first hit."""
    return next((seed for seed in range(limit) if broken(run(seed, profile))), None)


# -- what must work -------------------------------------------------------------------


def test_a_perfect_network_keeps_the_copies_identical() -> None:
    for seed in range(20):
        result = run(seed, PERFECT)
        assert result.quiescent, f"seed {seed} never settled"
        assert result.agrees, f"seed {seed} diverged with no faults at all: {result.summary()}"
        assert result.kept_its_promises, result.summary()


def test_the_primary_acks_every_write_when_nothing_is_wrong() -> None:
    result = run(SEED, PERFECT)
    assert result.acked == result.issued
    assert result.refused == 0


def test_replication_actually_reaches_the_backups() -> None:
    """A replicator that never sends anything would pass every agreement test by
    holding three empty maps. It would also pass by holding three copies of nothing."""
    result = run(SEED, PERFECT)
    for node_id, snapshot in result.stores.items():
        assert snapshot, f"node {node_id} holds nothing at all"


def test_the_client_is_told_yes_before_anything_leaves_the_machine() -> None:
    """The naivety, stated as a test. `on_client_request` returns True synchronously,
    at a moment when exactly one machine has the data."""
    cluster = NaiveCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=1)
    primary = cluster.sim.nodes[PRIMARY]
    backups = [cluster.sim.nodes[node] for node in (2, 3)]

    assert primary.on_client_request(Command.put("x", "1")) is True
    assert primary.kv.get("x") == "1"
    assert all(backup.kv.get("x") is None for backup in backups), (
        "the backups already have it, so the ack was not immediate"
    )


def test_a_crashed_primary_refuses_writes() -> None:
    cluster = NaiveCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=1)
    primary = cluster.sim.nodes[PRIMARY]
    primary.crash()
    assert primary.on_client_request(Command.put("x", "1")) is False


def test_a_backup_refuses_writes_even_when_healthy() -> None:
    """There is no forwarding and no leader lookup. A backup is not a primary."""
    cluster = NaiveCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=1)
    assert cluster.sim.nodes[2].on_client_request(Command.put("x", "1")) is False


def test_a_restarted_node_comes_back_from_its_log() -> None:
    cluster = NaiveCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=1)
    primary = cluster.sim.nodes[PRIMARY]
    primary.on_client_request(Command.put("x", "1"))
    primary.storage.sync()

    primary.crash()
    assert primary.kv.get("x") is None, "a crash must take volatile state with it"

    primary.restart()
    assert primary.kv.get("x") == "1", "a synced write must survive"
    assert primary.running


# -- what must NOT work ---------------------------------------------------------------


def test_variable_latency_alone_is_enough_to_break_it() -> None:
    """**The milestone.**

    `quiet` drops nothing, partitions nothing, crashes nothing and has a disk that never
    fails. The only thing it does is let one message take longer than another. Find the
    seeds where that is enough to leave three copies of the same data disagreeing.

    If this fails, look at the trace before assuming the harness is wrong -- it is also
    possible you accidentally wrote something correct, in which case find out what and
    write it in notes/rung3.md, because you have discovered a Raft rule on your own.
    """
    broken = first_seed_where(QUIET, lambda result: not result.agrees)
    assert broken is not None, "200 seeds of reordering and the copies never disagreed once"


def test_faults_lose_writes_the_client_was_promised() -> None:
    """Divergence is the weak failure. This is the one that matters: a value somebody
    was told was saved, that is not there."""
    lost = first_seed_where(AGGRESSIVE, lambda result: not result.kept_its_promises)
    assert lost is not None, "200 aggressive seeds and not one acknowledged write went missing"


def test_a_failing_seed_replays_identically() -> None:
    """Every failure this milestone finds must be reproducible on demand, or the whole
    harness was pointless."""
    broken = first_seed_where(QUIET, lambda result: not result.agrees)
    assert broken is not None
    assert run(broken, QUIET) == run(broken, QUIET)
