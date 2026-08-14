"""One mutant per checker.

    "An invariant that has never caught anything is not known to work." -- TODO.md

The checkers here are handed a cluster state that violates the property, built by hand,
and must say so. Two things make this worth doing rather than assuming:

1. A checker can be subtly inverted, or compare the wrong pair of things, and stay
   silent forever. Every seed then passes and the sweep reports success.
2. A checker can be *too* eager and fire on healthy behaviour, which is worse -- the
   usual response is to weaken it until it stops complaining, and then it detects
   nothing while looking like it does.

So every checker gets both: a state it must reject and a state it must accept.

These need no Raft implementation. They construct nodes directly, which is also why they
run today while `raft/node.py` is still unwritten -- the checkers are testable
independently of the thing they check, and that is the point of separating them.
"""

from collections.abc import Sequence

import pytest
from conftest import ListTrace

from groundhog.clock import SimClock
from groundhog.invariants.base import ClusterView, Invariants, InvariantViolated, Violation
from groundhog.invariants.history import ClientHistory
from groundhog.invariants.safety import (
    ElectionSafety,
    LeaderAppendOnly,
    LeaderCompleteness,
    LogMatching,
    MonotonicProgress,
    StateMachineSafety,
    all_checkers,
)
from groundhog.kv import Command
from groundhog.log import LogEntry, RaftLog
from groundhog.raft.node import IMPLEMENTED, RaftNode, Role
from groundhog.raft.persist import RaftStorage
from groundhog.raft.world import NO_ELECTION_RESTRICTION, run_raft
from groundhog.sim.disk import SimStorage
from groundhog.sim.event import EventQueue
from groundhog.sim.faults import FaultProfile
from groundhog.sim.rng import Rng
from groundhog.sim.trace import NullTrace
from groundhog.types import Index, NodeId, Term


class Nowhere:
    """A network nothing comes back from. These tests never send."""

    def send(self, to: NodeId, msg: object) -> None:
        return None


def make_node(
    node_id: NodeId,
    *,
    role: Role = Role.FOLLOWER,
    term: Term = 1,
    log: Sequence[LogEntry] = (),
    commit_index: Index = 0,
    last_applied: Index = 0,
    running: bool = True,
) -> RaftNode:
    queue = EventQueue()
    clock = SimClock(queue)
    rng = Rng(1)
    node = RaftNode(
        node_id=node_id,
        peers=(),
        clock=clock,
        net=Nowhere(),
        storage=RaftStorage(SimStorage(rng)),
        rng=rng,
        trace=ListTrace(),
    )
    node.role = role
    node.current_term = term
    node.log = RaftLog(log)
    node.commit_index = commit_index
    node.last_applied = last_applied
    node.running = running
    return node


def entry(index: Index, term: Term, value: str = "v") -> LogEntry:
    return LogEntry(term=term, index=index, command=Command.put("k", value))


def view(*nodes: RaftNode, tick: int = 100, history: ClientHistory | None = None) -> ClusterView:
    return ClusterView(
        tick=tick,
        nodes={node.node_id: node for node in nodes},
        history=history if history is not None else ClientHistory(),
    )


# =====================================================================================
# Election safety
# =====================================================================================


def test_election_safety_accepts_one_leader() -> None:
    checker = ElectionSafety()
    assert checker.observe(view(make_node(1, role=Role.LEADER, term=3), make_node(2))) is None


def test_election_safety_catches_two_leaders_in_one_term() -> None:
    checker = ElectionSafety()
    problem = checker.observe(
        view(
            make_node(1, role=Role.LEADER, term=3),
            make_node(2, role=Role.LEADER, term=3),
        )
    )
    assert problem is not None
    assert "two leaders" in problem


def test_election_safety_catches_a_second_leader_appearing_later() -> None:
    """The realistic shape: one is elected, then another claims the same term after a
    partition heals."""
    checker = ElectionSafety()
    assert checker.observe(view(make_node(1, role=Role.LEADER, term=5))) is None
    problem = checker.observe(view(make_node(2, role=Role.LEADER, term=5)))
    assert problem is not None


def test_two_leaders_in_different_terms_are_fine() -> None:
    """A deposed leader behind a partition still thinks it is in charge. That is not a
    bug -- it is why Raft ties authority to a term instead of a role."""
    checker = ElectionSafety()
    assert (
        checker.observe(
            view(
                make_node(1, role=Role.LEADER, term=3),
                make_node(2, role=Role.LEADER, term=4),
            )
        )
        is None
    )


def test_a_crashed_leader_is_not_a_leader() -> None:
    checker = ElectionSafety()
    assert checker.observe(view(make_node(1, role=Role.LEADER, term=3))) is None
    assert checker.observe(view(make_node(2, role=Role.LEADER, term=3, running=False))) is None


# =====================================================================================
# Log matching
# =====================================================================================


def test_log_matching_accepts_identical_logs() -> None:
    entries = [entry(1, 1), entry(2, 1), entry(3, 2)]
    checker = LogMatching()
    assert checker.observe(view(make_node(1, log=entries), make_node(2, log=entries))) is None


def test_log_matching_accepts_a_shorter_prefix() -> None:
    """A follower that is simply behind agrees about everything it has."""
    entries = [entry(1, 1), entry(2, 1), entry(3, 2)]
    checker = LogMatching()
    assert checker.observe(view(make_node(1, log=entries), make_node(2, log=entries[:1]))) is None


def test_log_matching_accepts_a_clean_divergence() -> None:
    """Diverging after a common prefix is normal -- it is what conflict resolution
    exists to repair. The property only forbids *rejoining* after diverging."""
    checker = LogMatching()
    left = [entry(1, 1), entry(2, 2)]
    right = [entry(1, 1), entry(2, 3)]
    assert checker.observe(view(make_node(1, log=left), make_node(2, log=right))) is None


def test_log_matching_catches_the_same_term_holding_different_commands() -> None:
    """★ Two entries with the same index and term must be the same entry. If they are
    not, two different leaders created entries in the same term -- which cannot happen
    if election safety holds, so this firing usually means that broke first."""
    checker = LogMatching()
    left = [entry(1, 1), entry(2, 2, "left")]
    right = [entry(1, 1), entry(2, 2, "right")]
    problem = checker.observe(view(make_node(1, log=left), make_node(2, log=right)))
    assert problem is not None
    assert "index 2" in problem
    assert "different commands" in problem


def test_log_matching_catches_a_rejoin_after_a_divergence() -> None:
    """★ The other shape: the logs legitimately diverge at index 2, and then *agree*
    again at index 3. Agreement after a divergence is impossible under Raft -- the
    consistency check exists precisely to make it so."""
    checker = LogMatching()
    left = [entry(1, 1), entry(2, 2, "left"), entry(3, 5)]
    right = [entry(1, 1), entry(2, 3, "right"), entry(3, 5)]
    problem = checker.observe(view(make_node(1, log=left), make_node(2, log=right)))
    assert problem is not None
    assert "index 3" in problem
    assert "differ from index 2" in problem


def test_log_matching_ignores_empty_logs() -> None:
    assert LogMatching().observe(view(make_node(1), make_node(2))) is None


# =====================================================================================
# Leader append-only
# =====================================================================================


def test_leader_append_only_accepts_growth() -> None:
    checker = LeaderAppendOnly()
    leader = make_node(1, role=Role.LEADER, log=[entry(1, 1)])
    assert checker.observe(view(leader)) is None

    leader.log.append([entry(2, 1)])
    assert checker.observe(view(leader)) is None


def test_leader_append_only_catches_a_leader_truncating_itself() -> None:
    """★ A leader that deletes its own entries has accepted an AppendEntries it should
    have refused -- the consistency check going wrong one step before anything else
    notices."""
    checker = LeaderAppendOnly()
    leader = make_node(1, role=Role.LEADER, log=[entry(1, 1), entry(2, 1)])
    assert checker.observe(view(leader)) is None

    leader.log.truncate_from(2)
    problem = checker.observe(view(leader))
    assert problem is not None
    assert "changed entries" in problem


def test_a_follower_may_be_truncated_freely() -> None:
    checker = LeaderAppendOnly()
    follower = make_node(1, log=[entry(1, 1), entry(2, 1)])
    assert checker.observe(view(follower)) is None
    follower.log.truncate_from(1)
    assert checker.observe(view(follower)) is None


def test_a_node_that_stops_leading_starts_over() -> None:
    """After being deposed and re-elected, a node may legitimately have been truncated
    by whoever replaced it."""
    checker = LeaderAppendOnly()
    node = make_node(1, role=Role.LEADER, log=[entry(1, 1), entry(2, 1)])
    assert checker.observe(view(node)) is None

    node.role = Role.FOLLOWER
    assert checker.observe(view(node)) is None
    node.log.truncate_from(2)

    node.role = Role.LEADER
    assert checker.observe(view(node)) is None


# =====================================================================================
# Leader completeness  --  the one the election restriction guarantees
# =====================================================================================


def test_leader_completeness_accepts_a_leader_holding_everything_committed() -> None:
    checker = LeaderCompleteness()
    committed = [entry(1, 1), entry(2, 1)]
    assert (
        checker.observe(
            view(
                make_node(1, log=committed, commit_index=2),
                make_node(2, role=Role.LEADER, term=2, log=committed),
            )
        )
        is None
    )


def test_leader_completeness_catches_a_leader_missing_a_committed_entry() -> None:
    """★★ This is the failure the election restriction (§5.4.1) prevents, and the
    reason spec §2's worked example matters.

    Node 1 committed index 2. Node 2 then becomes leader in a later term without it --
    which is exactly what happens if a voter grants its vote to a candidate whose log is
    behind. The entry a client was told was safe is now absent from the machine in
    charge, and will be overwritten.
    """
    checker = LeaderCompleteness()
    checker.observe(view(make_node(1, log=[entry(1, 1), entry(2, 1)], commit_index=2)))

    problem = checker.observe(view(make_node(2, role=Role.LEADER, term=5, log=[entry(1, 1)])))
    assert problem is not None
    assert "without committed entry" in problem


def test_leader_completeness_catches_a_committed_entry_replaced_by_another() -> None:
    checker = LeaderCompleteness()
    checker.observe(view(make_node(1, log=[entry(1, 1), entry(2, 1, "original")], commit_index=2)))

    problem = checker.observe(
        view(make_node(2, role=Role.LEADER, term=5, log=[entry(1, 1), entry(2, 4, "different")]))
    )
    assert problem is not None


def test_an_uncommitted_entry_may_be_lost() -> None:
    """Only *committed* entries are owed to anyone. Losing an uncommitted one is how
    Raft repairs a divergence, not a violation."""
    checker = LeaderCompleteness()
    checker.observe(view(make_node(1, log=[entry(1, 1), entry(2, 1)], commit_index=1)))
    assert checker.observe(view(make_node(2, role=Role.LEADER, term=5, log=[entry(1, 1)]))) is None


# =====================================================================================
# State machine safety
# =====================================================================================


def test_state_machine_safety_accepts_agreeing_applications() -> None:
    entries = [entry(1, 1), entry(2, 1)]
    checker = StateMachineSafety()
    assert (
        checker.observe(
            view(
                make_node(1, log=entries, commit_index=2, last_applied=2),
                make_node(2, log=entries, commit_index=2, last_applied=2),
            )
        )
        is None
    )


def test_state_machine_safety_catches_two_nodes_applying_different_entries() -> None:
    """★ The property the outside world can feel: two servers applied different commands
    at the same index, so two clients reading the same key get different answers and
    both are 'correct'."""
    checker = StateMachineSafety()
    problem = checker.observe(
        view(
            make_node(1, log=[entry(1, 1, "mine")], commit_index=1, last_applied=1),
            make_node(2, log=[entry(1, 1, "yours")], commit_index=1, last_applied=1),
        )
    )
    assert problem is not None
    assert "index 1" in problem


def test_state_machine_safety_catches_an_entry_changing_after_it_was_applied() -> None:
    """★ Worse than disagreement: a single node applied one thing and now holds another
    at that index. Whatever it told a client is no longer true of its own log."""
    checker = StateMachineSafety()
    node = make_node(1, log=[entry(1, 1, "first")], commit_index=1, last_applied=1)
    assert checker.observe(view(node)) is None

    node.log = RaftLog([entry(1, 2, "rewritten")])
    problem = checker.observe(view(node))
    assert problem is not None


def test_state_machine_safety_catches_applying_past_the_end_of_the_log() -> None:
    checker = StateMachineSafety()
    problem = checker.observe(view(make_node(1, log=[entry(1, 1)], last_applied=3)))
    assert problem is not None
    assert "stops at" in problem


def test_lagging_is_not_losing() -> None:
    """A follower behind the leader holds an older applied value. That is lag. A checker
    that called it loss would fire on healthy runs, and would then get weakened until it
    caught nothing."""
    entries = [entry(1, 1), entry(2, 1)]
    checker = StateMachineSafety()
    assert (
        checker.observe(
            view(
                make_node(1, log=entries, commit_index=2, last_applied=2),
                make_node(2, log=entries, commit_index=1, last_applied=1),
            )
        )
        is None
    )


# =====================================================================================
# Monotonic progress
# =====================================================================================


def test_progress_accepts_moving_forward() -> None:
    checker = MonotonicProgress()
    node = make_node(1, log=[entry(1, 1), entry(2, 1)], commit_index=1, last_applied=1)
    assert checker.observe(view(node)) is None
    node.commit_index = 2
    assert checker.observe(view(node)) is None


def test_progress_catches_a_commit_index_going_backwards() -> None:
    checker = MonotonicProgress()
    node = make_node(1, log=[entry(1, 1), entry(2, 1)], commit_index=2)
    assert checker.observe(view(node)) is None
    node.commit_index = 1
    problem = checker.observe(view(node))
    assert problem is not None
    assert "backwards" in problem


def test_progress_catches_applying_more_than_was_committed() -> None:
    checker = MonotonicProgress()
    problem = checker.observe(view(make_node(1, log=[entry(1, 1)], commit_index=0, last_applied=1)))
    assert problem is not None
    assert "committed" in problem


def test_a_crash_resets_progress_legitimately() -> None:
    checker = MonotonicProgress()
    node = make_node(1, log=[entry(1, 1)], commit_index=1)
    assert checker.observe(view(node)) is None

    node.running = False
    node.commit_index = 0
    assert checker.observe(view(node)) is None

    node.running = True
    assert checker.observe(view(node)) is None


# =====================================================================================
# The registry
# =====================================================================================


def test_the_registry_stays_quiet_on_a_healthy_cluster() -> None:
    entries = [entry(1, 1), entry(2, 1)]
    registry = Invariants(all_checkers(), seed=4471)
    registry.observe(
        view(
            make_node(1, role=Role.LEADER, term=1, log=entries, commit_index=2, last_applied=2),
            make_node(2, log=entries, commit_index=2, last_applied=2),
            make_node(3, log=entries[:1], commit_index=1, last_applied=1),
        )
    )
    assert registry.ok


def test_the_registry_reports_the_seed_and_the_tick() -> None:
    registry = Invariants(all_checkers(), seed=4471)
    try:
        registry.observe(
            view(
                make_node(1, role=Role.LEADER, term=3),
                make_node(2, role=Role.LEADER, term=3),
                tick=123_456,
            )
        )
    except InvariantViolated as caught:
        violation = caught.violation
    else:  # pragma: no cover - the observe above must raise
        raise AssertionError("the registry did not fire")

    assert violation.seed == 4471
    assert violation.tick == 123_456
    assert violation.checker == "election_safety"
    assert "--seed 4471" in violation.reproduce()


def test_the_registry_can_carry_on_instead_of_stopping() -> None:
    registry = Invariants([ElectionSafety()], seed=1, stop_on_violation=False)
    registry.observe(
        view(make_node(1, role=Role.LEADER, term=3), make_node(2, role=Role.LEADER, term=3))
    )
    assert not registry.ok
    assert len(registry.violations) == 1


def test_a_stride_checks_less_often() -> None:
    """M7's lever when a sweep is throughput-bound. The cost is precision: the tick in
    the report is the tick it was *noticed*, not the tick it broke."""
    registry = Invariants([ElectionSafety()], seed=1, stride=3, stop_on_violation=False)
    broken = view(make_node(1, role=Role.LEADER, term=3), make_node(2, role=Role.LEADER, term=3))
    registry.observe(broken)
    registry.observe(broken)
    assert registry.ok
    registry.observe(broken)
    assert not registry.ok


def test_a_violation_describes_itself_and_how_to_replay_it() -> None:
    violation = Violation(checker="election_safety", seed=847392, tick=99, detail="two leaders")
    assert violation.describe() == "[election_safety] seed 847392 tick 99: two leaders"
    assert "groundhog raft --seed 847392" in violation.reproduce()


# =====================================================================================
# The done-when: break Raft for real, and watch a sweep find it
# =====================================================================================

needs_raft = pytest.mark.skipif(
    not IMPLEMENTED,
    reason="M5 [Y]: write the six functions in groundhog/raft/node.py, then set IMPLEMENTED = True",
)


@needs_raft
def test_a_correct_raft_violates_nothing() -> None:
    """No false positives. A checker that fires on healthy runs is worse than none --
    it trains you to ignore it."""
    for seed in range(40):
        result = run_raft(seed, trace=NullTrace(), profile=FaultProfile.aggressive(), writes=10)
        assert not result.violations, f"seed {seed}: {result.violations[0].describe()}"


@needs_raft
def test_removing_the_election_restriction_gets_caught() -> None:
    """★ **The M6 done-when.**

    `--mutate no-election-restriction` rewrites every incoming RequestVote to claim a
    maximally up-to-date log, so every voter's §5.4.1 comparison passes. That is exactly
    what deleting the restriction does, without touching `raft/node.py`.

    Leader completeness is what should notice: a node with a stale log wins an election,
    and an entry a client was told was safe is not in the log of the machine now in
    charge.
    """
    for seed in range(200):
        result = run_raft(
            seed,
            trace=NullTrace(),
            profile=FaultProfile.aggressive(),
            writes=10,
            mutate=NO_ELECTION_RESTRICTION,
        )
        if not result.violations:
            continue

        violation = result.violations[0]
        assert violation.checker in ("leader_completeness", "state_machine_safety"), (
            f"the restriction was removed and {violation.checker} fired instead"
        )

        replayed = run_raft(
            seed,
            trace=NullTrace(),
            profile=FaultProfile.aggressive(),
            writes=10,
            mutate=NO_ELECTION_RESTRICTION,
        )
        assert replayed.violations == result.violations, "the reported seed did not replay"
        return

    raise AssertionError("200 seeds with the election restriction removed and nothing fired")


@needs_raft
def test_the_bug_is_invisible_without_faults() -> None:
    """Worth knowing, and slightly alarming: with the election restriction removed and a
    merely *slow* network, 60 seeds find nothing.

    No node ever falls far enough behind for the rule to matter. The bug needs a
    partition or a crash to be reachable at all -- which is spec §2's whole argument for
    why normal testing cannot find it, demonstrated rather than asserted.
    """
    violations = [
        seed
        for seed in range(60)
        if run_raft(
            seed,
            trace=NullTrace(),
            profile=FaultProfile.quiet(),
            writes=10,
            mutate=NO_ELECTION_RESTRICTION,
        ).violations
    ]
    assert not violations, (
        "a merely slow network now finds this. Good news, but the docstring above and "
        "the M10 writeup both need updating."
    )
