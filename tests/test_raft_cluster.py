"""M5's done-when, as tests.

    a 3-node cluster elects a leader, replicates writes, survives leader crash, and
    recovers -- under a clean network first, then under faults.

Skipped until `raft/node.py` says `IMPLEMENTED = True`. Get `test_raft_node.py` green
first; a failure here tells you something is wrong, a failure there tells you what.
"""

from itertools import combinations

import pytest
from conftest import ListTrace

from groundhog.raft.node import IMPLEMENTED, Role
from groundhog.raft.world import NODE_IDS, RaftCluster, RaftResult, run_raft
from groundhog.sim.faults import FaultProfile
from groundhog.sim.net import NodeState
from groundhog.sim.trace import NullTrace
from groundhog.types import SECOND, NodeId

pytestmark = pytest.mark.skipif(
    not IMPLEMENTED,
    reason="M5 [Y]: write the six functions in groundhog/raft/node.py, then set IMPLEMENTED = True",
)

PERFECT = FaultProfile.perfect()
QUIET = FaultProfile.quiet()
AGGRESSIVE = FaultProfile.aggressive()
SEED = 4471


def run(seed: int, profile: FaultProfile, writes: int = 20) -> RaftResult:
    return run_raft(seed, trace=NullTrace(), profile=profile, writes=writes)


def settled(cluster: RaftCluster) -> list[NodeId]:
    return cluster.leader_ids()


# -- elects a leader ------------------------------------------------------------------


def test_a_cluster_elects_a_leader() -> None:
    cluster = RaftCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=1)
    cluster.start()
    cluster.sim.run(3 * SECOND)
    assert len(settled(cluster)) == 1


def test_a_leader_is_elected_from_every_starting_seed() -> None:
    """Randomised timeouts are what break the tie. If some seed never elects anyone,
    the randomisation is not doing its job."""
    for seed in range(20):
        cluster = RaftCluster(seed, trace=NullTrace(), profile=PERFECT, writes=1)
        cluster.start()
        cluster.sim.run(5 * SECOND)
        assert settled(cluster), f"seed {seed} elected nobody in 5 seconds"


def test_nobody_holds_a_term_twice() -> None:
    """Election safety in its crudest form. M6 checks this properly, across the whole
    run rather than at the end."""
    cluster = RaftCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=5)
    cluster.run()
    leaders = settled(cluster)
    terms = [cluster.node(node_id).current_term for node_id in leaders]
    assert len(terms) == len(dict.fromkeys(terms))


# -- replicates writes ----------------------------------------------------------------


def test_every_write_is_acknowledged_on_a_clean_network() -> None:
    result = run(SEED, PERFECT)
    assert result.acked == result.requested


def test_acknowledged_writes_are_on_every_node() -> None:
    """The rung-3 failure, checked against Raft: what the client was promised is what
    all three copies hold."""
    cluster = RaftCluster(SEED, trace=NullTrace(), profile=PERFECT)
    cluster.run()

    expected: dict[str, str] = {}
    for command in cluster.client.acked:
        expected[command.key] = command.value

    for node_id in NODE_IDS:
        store = cluster.node(node_id).kv
        for key, value in expected.items():
            assert store.get(key) == value, f"node {node_id} is missing {key}={value}"


def test_the_commit_index_is_the_same_everywhere_once_it_settles() -> None:
    result = run(SEED, PERFECT)
    assert len(dict.fromkeys(result.committed.values())) == 1


def test_a_follower_refuses_to_serve_writes() -> None:
    """And says who to ask instead, which is how the client finds the leader at all."""
    cluster = RaftCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=3)
    cluster.run()
    assert cluster.client.attempts >= cluster.client.writes


# -- survives leader crash ------------------------------------------------------------


def test_the_cluster_survives_losing_its_leader() -> None:
    """The done-when, directly: elect, kill the leader, elect again."""
    cluster = RaftCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=20)
    cluster.start()
    cluster.sim.run(3 * SECOND)

    leaders = settled(cluster)
    assert leaders, "nobody was elected in the first place"
    victim = leaders[0]

    cluster.sim.nodes[victim].crash()
    cluster.network.set_state(victim, NodeState.CRASHED)

    cluster.sim.run(15 * SECOND)
    survivors = [
        node_id
        for node_id in NODE_IDS
        if node_id != victim and cluster.node(node_id).role is Role.LEADER
    ]
    assert survivors, "the leader died and nobody took over"


def test_writes_keep_being_acknowledged_after_a_leader_dies() -> None:
    # Enough writes that the client is still working when the leader dies -- otherwise
    # this measures nothing except that the client had already finished.
    cluster = RaftCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=100)
    cluster.start()
    cluster.sim.run(1 * SECOND)

    before = len(cluster.client.acked)
    assert 0 < before < cluster.client.writes, "the test window is wrong, not the cluster"

    victim = settled(cluster)[0]
    cluster.sim.nodes[victim].crash()
    cluster.network.set_state(victim, NodeState.CRASHED)

    cluster.sim.run(30 * SECOND)
    assert len(cluster.client.acked) > before, "the cluster stopped serving after one death"


def test_a_restarted_node_catches_up() -> None:
    cluster = RaftCluster(SEED, trace=ListTrace(), profile=PERFECT, writes=20)
    cluster.start()
    cluster.sim.run(3 * SECOND)

    victim = settled(cluster)[0]
    cluster.sim.nodes[victim].crash()
    cluster.network.set_state(victim, NodeState.CRASHED)
    cluster.sim.run(8 * SECOND)

    cluster.sim.nodes[victim].restart()
    cluster.network.set_state(victim, NodeState.RUNNING)
    cluster.sim.run(20 * SECOND)

    healthy = max(cluster.node(n).commit_index for n in NODE_IDS)
    assert cluster.node(victim).commit_index == healthy, "the restarted node never caught up"


# -- under faults ---------------------------------------------------------------------


def test_committed_entries_never_disagree() -> None:
    """★ Log matching, on the committed prefix, across 30 hostile universes.

    Note what this does *not* assert. "Every node holds the last acknowledged value for
    every key" is false and should be: a follower that is merely behind holds an older
    acknowledged value, which is lag, not loss. Asserting it would produce a test that
    fails on healthy behaviour -- and the usual response to that is to weaken the test
    until it passes, which is how a suite ends up proving nothing.

    What must never happen is two nodes holding *different* entries at the same
    committed index. That is a real safety violation with no benign explanation.

    M6 does this properly: after every event rather than once at the end, and with a
    client history that can express "acknowledged then lost".
    """
    for seed in range(30):
        cluster = RaftCluster(seed, trace=NullTrace(), profile=AGGRESSIVE, writes=10)
        cluster.run()

        nodes = [cluster.node(node_id) for node_id in NODE_IDS]
        for left, right in combinations(nodes, 2):
            shared = min(left.commit_index, right.commit_index)
            for index in range(1, shared + 1):
                assert left.log.entry_at(index) == right.log.entry_at(index), (
                    f"seed {seed}: n{left.node_id} and n{right.node_id} disagree at "
                    f"committed index {index}"
                )


def test_the_cluster_still_makes_progress_under_faults() -> None:
    acked = [run(seed, QUIET, writes=10).acked for seed in range(10)]
    assert sum(acked) > 0, "10 seeds of a merely slow network and not one write landed"


def test_a_raft_run_replays_identically() -> None:
    first = run(SEED, AGGRESSIVE)
    second = run(SEED, AGGRESSIVE)
    assert first == second
