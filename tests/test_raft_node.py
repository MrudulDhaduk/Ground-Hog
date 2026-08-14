"""The tests the six functions in `raft/node.py` have to pass.

Skipped until you set `IMPLEMENTED = True`. Run `pytest tests/test_raft_node.py -x` as
you go; they are ordered roughly the way the paper is, and roughly the order that lets
you get a cluster running soonest:

    on_election_timeout -> on_request_vote -> on_request_vote_reply
    -> on_append_entries -> on_append_entries_reply -> advance_commit_index

Tests marked ★ in their docstring are the ones covering rules whose absence produces a
system that works perfectly until it silently loses data.

Nothing here tests a whole cluster; `test_raft_cluster.py` does that. These drive one
node with one message at a time, because a failure in a cluster test tells you something
is wrong and a failure here tells you what.
"""

from collections.abc import Sequence

import pytest
from conftest import ListTrace

from groundhog.clock import SimClock, SimTimer
from groundhog.kv import Command
from groundhog.log import LogEntry, RaftLog
from groundhog.messages import (
    AppendEntries,
    AppendEntriesReply,
    ClientReply,
    ClientRequest,
    Message,
    RequestVote,
    RequestVoteReply,
)
from groundhog.raft.node import IMPLEMENTED, RaftNode, Role
from groundhog.raft.persist import HardState, RaftStorage, replay
from groundhog.sim.disk import SimStorage
from groundhog.sim.event import EventQueue
from groundhog.sim.rng import Rng
from groundhog.types import Index, NodeId, Term

pytestmark = pytest.mark.skipif(
    not IMPLEMENTED,
    reason="M5 [Y]: write the six functions in groundhog/raft/node.py, then set IMPLEMENTED = True",
)


def entry(index: Index, term: Term, value: str = "v") -> LogEntry:
    return LogEntry(term=term, index=index, command=Command.put(f"k{index}", value))


def entries_with_terms(*terms: Term) -> list[LogEntry]:
    return [entry(index, term) for index, term in enumerate(terms, start=1)]


class CaptureNet:
    """A network that records what was sent -- and, at the moment of sending, what was
    already durable on disk.

    That second half is how the persistence-ordering rule becomes testable at all.
    Figure 2 says persistent state is updated *before responding to RPCs*; checking the
    disk after the fact cannot tell "synced then replied" from "replied then synced".
    Snapshotting at send time can.
    """

    def __init__(self, disk: SimStorage) -> None:
        self.disk = disk
        self.sent: list[tuple[NodeId, Message]] = []
        self.durable_at_send: list[HardState] = []

    def send(self, to: NodeId, msg: Message) -> None:
        self.sent.append((to, msg))
        self.durable_at_send.append(replay(self.disk.durable_records()).state)


class Harness:
    def __init__(
        self,
        *,
        node_id: NodeId = 1,
        peers: tuple[NodeId, ...] = (2, 3),
        term: Term = 0,
        voted_for: NodeId | None = None,
        log_terms: Sequence[Term] = (),
        seed: int = 1,
    ) -> None:
        self.queue = EventQueue()
        self.clock = SimClock(self.queue)
        self.rng = Rng(seed)
        self.trace = ListTrace()
        self.disk = SimStorage(self.rng)
        self.net = CaptureNet(self.disk)

        self.node = RaftNode(
            node_id=node_id,
            peers=peers,
            clock=self.clock,
            net=self.net,
            storage=RaftStorage(self.disk),
            rng=self.rng,
            trace=self.trace,
        )
        self.node.current_term = term
        self.node.voted_for = voted_for
        self.node.log = RaftLog(entries_with_terms(*log_terms))
        self.node.start()

    # -- driving --------------------------------------------------------------

    def drain(self, limit: int = 1000) -> None:
        for _ in range(limit):
            event = self.queue.pop()
            if event is None:
                return
            self.clock.advance_to(event.tick)
            event.action()

    def fire_election_timeout(self) -> None:
        self.node.on_election_timeout()

    # -- inspecting -----------------------------------------------------------

    def sent_to(self, node_id: NodeId) -> list[Message]:
        return [msg for to, msg in self.net.sent if to == node_id]

    def only_reply(self) -> Message:
        assert len(self.net.sent) == 1, f"expected exactly one message, got {self.net.sent}"
        return self.net.sent[0][1]

    def durable_when_replying(self) -> HardState:
        assert self.net.durable_at_send, "nothing was sent"
        return self.net.durable_at_send[-1]

    def election_timer(self) -> SimTimer:
        timer = self.node.election_timer
        assert isinstance(timer, SimTimer), "the election timer is not armed"
        return timer


# =====================================================================================
# on_election_timeout
# =====================================================================================


def test_a_timeout_starts_an_election() -> None:
    world = Harness(term=4)
    world.fire_election_timeout()

    assert world.node.role is Role.CANDIDATE
    assert world.node.current_term == 5
    assert world.node.voted_for == world.node.node_id


def test_a_candidate_advertises_its_log_to_everyone() -> None:
    world = Harness(term=1, log_terms=(1, 1, 3))
    world.fire_election_timeout()

    for peer in (2, 3):
        votes = [m for m in world.sent_to(peer) if isinstance(m, RequestVote)]
        assert len(votes) == 1, f"peer {peer} got {votes}"
        assert votes[0] == RequestVote(term=2, candidate_id=1, last_log_index=3, last_log_term=3)


def test_an_election_resets_the_timer_so_it_can_try_again() -> None:
    world = Harness()
    before = world.election_timer()
    world.fire_election_timeout()
    assert world.node.election_timer is not before


def test_a_second_timeout_starts_a_fresh_election() -> None:
    world = Harness()
    world.fire_election_timeout()
    world.fire_election_timeout()
    assert world.node.current_term == 2
    assert world.node.role is Role.CANDIDATE


def test_the_term_and_self_vote_are_durable_before_the_votes_go_out() -> None:
    """★ Figure 2: persistent state is updated *before responding to RPCs*. A candidate
    that crashes here and forgets it voted for itself can vote again in the same term."""
    world = Harness(term=4)
    world.fire_election_timeout()
    assert world.durable_when_replying() == HardState(current_term=5, voted_for=1)


def test_a_lone_node_elects_itself() -> None:
    world = Harness(peers=())
    world.fire_election_timeout()
    assert world.node.role is Role.LEADER


# =====================================================================================
# on_request_vote  --  including the election restriction
# =====================================================================================


def grant(world: Harness) -> bool:
    reply = world.only_reply()
    assert isinstance(reply, RequestVoteReply)
    return reply.vote_granted


def test_a_fresh_follower_grants_its_vote() -> None:
    world = Harness(term=1)
    world.node.on_request_vote(
        2, RequestVote(term=2, candidate_id=2, last_log_index=0, last_log_term=0)
    )
    assert grant(world)
    assert world.node.voted_for == 2
    assert world.node.current_term == 2


def test_a_stale_candidate_is_refused() -> None:
    world = Harness(term=5)
    world.node.on_request_vote(
        2, RequestVote(term=3, candidate_id=2, last_log_index=9, last_log_term=5)
    )
    assert not grant(world)
    assert world.node.current_term == 5


def test_the_reply_carries_the_voters_term() -> None:
    world = Harness(term=5)
    world.node.on_request_vote(
        2, RequestVote(term=3, candidate_id=2, last_log_index=0, last_log_term=0)
    )
    reply = world.only_reply()
    assert isinstance(reply, RequestVoteReply)
    assert reply.term == 5


def test_one_vote_per_term() -> None:
    world = Harness(term=1)
    world.node.on_request_vote(
        2, RequestVote(term=2, candidate_id=2, last_log_index=0, last_log_term=0)
    )
    world.node.on_request_vote(
        3, RequestVote(term=2, candidate_id=3, last_log_index=0, last_log_term=0)
    )

    first, second = world.net.sent[0][1], world.net.sent[1][1]
    assert isinstance(first, RequestVoteReply) and isinstance(second, RequestVoteReply)
    assert first.vote_granted
    assert not second.vote_granted


def test_asking_twice_gets_the_same_answer() -> None:
    """A duplicated RequestVote is not a second candidate."""
    world = Harness(term=1)
    ask = RequestVote(term=2, candidate_id=2, last_log_index=0, last_log_term=0)
    world.node.on_request_vote(2, ask)
    world.node.on_request_vote(2, ask)

    replies = [m for _, m in world.net.sent if isinstance(m, RequestVoteReply)]
    assert [r.vote_granted for r in replies] == [True, True]


def test_a_higher_term_makes_a_leader_step_down() -> None:
    world = Harness(term=2)
    world.node.become_leader()
    world.node.on_request_vote(
        2, RequestVote(term=9, candidate_id=2, last_log_index=0, last_log_term=0)
    )
    assert world.node.role is Role.FOLLOWER
    assert world.node.current_term == 9


def test_the_election_restriction_refuses_an_older_log() -> None:
    """★ §5.4.1. The candidate's last entry is from term 1; ours is from term 3."""
    world = Harness(term=3, log_terms=(1, 2, 3))
    world.node.on_request_vote(
        2, RequestVote(term=4, candidate_id=2, last_log_index=1, last_log_term=1)
    )
    assert not grant(world)


def test_the_election_restriction_refuses_a_shorter_log_at_the_same_term() -> None:
    """★ §5.4.1: same last term, so the longer log wins."""
    world = Harness(term=3, log_terms=(1, 3, 3))
    world.node.on_request_vote(
        2, RequestVote(term=4, candidate_id=2, last_log_index=2, last_log_term=3)
    )
    assert not grant(world)


def test_the_election_restriction_compares_term_before_length() -> None:
    """★ The one that catches a length-first implementation.

    The candidate's log is *shorter* (2 entries against our 4) but its last entry is
    from a later term (5 against our 2). It is more up to date, and refusing it here is
    how a cluster deadlocks -- or worse, how the wrong node wins later.
    """
    world = Harness(term=5, log_terms=(1, 1, 2, 2))
    world.node.on_request_vote(
        2, RequestVote(term=6, candidate_id=2, last_log_index=2, last_log_term=5)
    )
    assert grant(world)


def test_an_equal_log_is_good_enough() -> None:
    """'At least as up to date', not 'strictly ahead'."""
    world = Harness(term=3, log_terms=(1, 2, 3))
    world.node.on_request_vote(
        2, RequestVote(term=4, candidate_id=2, last_log_index=3, last_log_term=3)
    )
    assert grant(world)


def test_an_empty_candidate_log_loses_to_a_non_empty_one() -> None:
    world = Harness(term=1, log_terms=(1,))
    world.node.on_request_vote(
        2, RequestVote(term=2, candidate_id=2, last_log_index=0, last_log_term=0)
    )
    assert not grant(world)


def test_the_vote_is_durable_before_the_reply_is_sent() -> None:
    """★ Grant a vote, crash, come back having forgotten it, grant it again -- and one
    term has two leaders."""
    world = Harness(term=1)
    world.node.on_request_vote(
        2, RequestVote(term=2, candidate_id=2, last_log_index=0, last_log_term=0)
    )
    assert grant(world)
    assert world.durable_when_replying() == HardState(current_term=2, voted_for=2)


# =====================================================================================
# on_request_vote_reply
# =====================================================================================


def candidate_at(term: Term, **kwargs: object) -> Harness:
    world = Harness(term=term - 1, **kwargs)  # type: ignore[arg-type]
    world.fire_election_timeout()
    world.net.sent.clear()
    world.net.durable_at_send.clear()
    return world


def test_a_majority_wins_the_election() -> None:
    world = candidate_at(1)
    world.node.on_request_vote_reply(2, RequestVoteReply(term=1, vote_granted=True))
    assert world.node.role is Role.LEADER


def test_a_single_vote_is_not_a_majority_of_five() -> None:
    world = candidate_at(1, peers=(2, 3, 4, 5))
    world.node.on_request_vote_reply(2, RequestVoteReply(term=1, vote_granted=True))
    assert world.node.role is Role.CANDIDATE


def test_a_refusal_does_not_count() -> None:
    world = candidate_at(1)
    world.node.on_request_vote_reply(2, RequestVoteReply(term=1, vote_granted=False))
    world.node.on_request_vote_reply(3, RequestVoteReply(term=1, vote_granted=False))
    assert world.node.role is Role.CANDIDATE


def test_the_same_voter_cannot_vote_twice() -> None:
    """★ The network duplicates messages. A duplicate is not a second vote, and a
    5-node cluster must not be won with two."""
    world = candidate_at(1, peers=(2, 3, 4, 5))
    reply = RequestVoteReply(term=1, vote_granted=True)
    world.node.on_request_vote_reply(2, reply)
    world.node.on_request_vote_reply(2, reply)
    assert world.node.role is Role.CANDIDATE


def test_a_reply_from_an_old_term_is_ignored() -> None:
    """It answers a question we stopped asking two elections ago."""
    world = candidate_at(3)
    world.node.on_request_vote_reply(2, RequestVoteReply(term=1, vote_granted=True))
    assert world.node.role is Role.CANDIDATE


def test_a_reply_from_a_higher_term_means_we_lost() -> None:
    world = candidate_at(2)
    world.node.on_request_vote_reply(2, RequestVoteReply(term=7, vote_granted=False))
    assert world.node.role is Role.FOLLOWER
    assert world.node.current_term == 7


def test_a_new_leader_starts_next_index_past_its_own_log() -> None:
    world = candidate_at(1, log_terms=(1, 1, 1))
    world.node.on_request_vote_reply(2, RequestVoteReply(term=1, vote_granted=True))

    assert world.node.role is Role.LEADER
    assert world.node.next_index == {2: 4, 3: 4}
    assert world.node.match_index == {2: 0, 3: 0}


def test_a_new_leader_announces_itself() -> None:
    world = candidate_at(1)
    world.node.on_request_vote_reply(2, RequestVoteReply(term=1, vote_granted=True))
    for peer in (2, 3):
        assert any(isinstance(m, AppendEntries) for m in world.sent_to(peer))


# =====================================================================================
# on_append_entries
# =====================================================================================


def result(world: Harness) -> AppendEntriesReply:
    reply = world.only_reply()
    assert isinstance(reply, AppendEntriesReply)
    return reply


def heartbeat(term: Term, *, commit: Index = 0, prev: tuple[Index, Term] = (0, 0)) -> AppendEntries:
    return AppendEntries(
        term=term,
        leader_id=2,
        prev_log_index=prev[0],
        prev_log_term=prev[1],
        entries=(),
        leader_commit=commit,
    )


def test_a_heartbeat_from_a_current_leader_is_accepted() -> None:
    world = Harness(term=2)
    world.node.on_append_entries(2, heartbeat(2))
    assert result(world).success
    assert world.node.leader_id == 2


def test_a_heartbeat_resets_the_election_timer() -> None:
    """Which is the entire reason heartbeats exist."""
    world = Harness(term=2)
    before = world.election_timer()
    world.node.on_append_entries(2, heartbeat(2))
    assert world.node.election_timer is not before


def test_a_leader_from_an_old_term_is_refused() -> None:
    world = Harness(term=5)
    world.node.on_append_entries(2, heartbeat(3))
    reply = result(world)
    assert not reply.success
    assert reply.term == 5


def test_a_stale_leader_does_not_reset_our_timer() -> None:
    world = Harness(term=5)
    before = world.election_timer()
    world.node.on_append_entries(2, heartbeat(3))
    assert world.node.election_timer is before


def test_a_candidate_steps_down_for_a_current_leader() -> None:
    world = candidate_at(3)
    world.node.on_append_entries(2, heartbeat(3))
    assert world.node.role is Role.FOLLOWER
    assert world.node.leader_id == 2


def test_the_consistency_check_refuses_a_gap() -> None:
    """★ Figure 2 rule 2: we have 2 entries, the leader assumes we have 5."""
    world = Harness(term=2, log_terms=(1, 1))
    world.node.on_append_entries(2, heartbeat(2, prev=(5, 2)))
    assert not result(world).success


def test_the_consistency_check_refuses_a_term_mismatch() -> None:
    """★ Same index, different term: our history and the leader's have diverged."""
    world = Harness(term=3, log_terms=(1, 1, 1))
    world.node.on_append_entries(2, heartbeat(3, prev=(3, 2)))
    assert not result(world).success


def test_entries_are_appended_after_a_matching_prefix() -> None:
    world = Harness(term=2, log_terms=(1,))
    world.node.on_append_entries(
        2,
        AppendEntries(
            term=2,
            leader_id=2,
            prev_log_index=1,
            prev_log_term=1,
            entries=(entry(2, 2), entry(3, 2)),
        ),
    )
    reply = result(world)
    assert reply.success
    assert reply.match_index == 3
    assert [(e.index, e.term) for e in world.node.log.entries()] == [(1, 1), (2, 2), (3, 2)]


def test_the_first_entry_ever_is_accepted_at_the_sentinel() -> None:
    world = Harness(term=1)
    world.node.on_append_entries(
        2,
        AppendEntries(
            term=1, leader_id=2, prev_log_index=0, prev_log_term=0, entries=(entry(1, 1),)
        ),
    )
    assert result(world).success
    assert world.node.log.last_index() == 1


def test_a_conflicting_entry_takes_its_successors_with_it() -> None:
    """★ Figure 2 rule 3. Our index 2 is from term 1; the leader's is from term 2, so
    ours and everything after it must go."""
    world = Harness(term=2, log_terms=(1, 1, 1))
    world.node.on_append_entries(
        2,
        AppendEntries(
            term=2, leader_id=2, prev_log_index=1, prev_log_term=1, entries=(entry(2, 2),)
        ),
    )
    assert result(world).success
    assert [(e.index, e.term) for e in world.node.log.entries()] == [(1, 1), (2, 2)]


def test_a_retransmission_does_not_truncate_a_matching_log() -> None:
    """★ The other half of rule 3, and the one that is easy to get wrong.

    Delete *on conflict*, not on every AppendEntries. This message repeats entries we
    already have and adds nothing; truncating here would throw away index 3 -- which may
    already be committed.
    """
    world = Harness(term=2, log_terms=(1, 2, 2))
    world.node.on_append_entries(
        2,
        AppendEntries(
            term=2, leader_id=2, prev_log_index=1, prev_log_term=1, entries=(entry(2, 2),)
        ),
    )
    assert result(world).success
    assert world.node.log.last_index() == 3, "a matching retransmission truncated the log"


def test_commit_index_follows_the_leader() -> None:
    world = Harness(term=2, log_terms=(1, 1, 1))
    world.node.on_append_entries(2, heartbeat(2, commit=2, prev=(3, 1)))
    assert world.node.commit_index == 2


def test_commit_index_never_runs_past_our_own_log() -> None:
    """★ min(leaderCommit, index of last new entry). A leader whose commit index is 9
    must not make a follower with 3 entries claim to have committed 9."""
    world = Harness(term=2, log_terms=(1,))
    world.node.on_append_entries(2, heartbeat(2, commit=9, prev=(1, 1)))
    assert world.node.commit_index <= 1


def test_committed_entries_reach_the_state_machine() -> None:
    world = Harness(term=2)
    world.node.on_append_entries(
        2,
        AppendEntries(
            term=2,
            leader_id=2,
            prev_log_index=0,
            prev_log_term=0,
            entries=(LogEntry(term=2, index=1, command=Command.put("x", "5")),),
            leader_commit=1,
        ),
    )
    assert world.node.kv.get("x") == "5"
    assert world.node.last_applied == 1


def test_entries_are_durable_before_the_reply_is_sent() -> None:
    """★ A follower that says "I have it" and then loses it has lied to the leader, and
    the leader may have already told a client the write is safe."""
    world = Harness(term=2)
    world.node.on_append_entries(
        2,
        AppendEntries(
            term=2, leader_id=2, prev_log_index=0, prev_log_term=0, entries=(entry(1, 2),)
        ),
    )
    assert result(world).success
    surviving = replay(world.disk.durable_records())
    assert len(surviving.entries) == 1, "the reply left before the entry was synced"


# =====================================================================================
# on_append_entries_reply
# =====================================================================================


def leader_with(*log_terms: Term, term: Term = 2) -> Harness:
    world = Harness(term=term, log_terms=log_terms)
    world.node.become_leader()
    world.net.sent.clear()
    world.net.durable_at_send.clear()
    return world


def test_success_advances_what_we_know_about_a_follower() -> None:
    world = leader_with(2, 2, 2)
    world.node.on_append_entries_reply(2, AppendEntriesReply(term=2, success=True, match_index=3))
    assert world.node.match_index[2] == 3
    assert world.node.next_index[2] == 4


def test_a_stale_success_does_not_drag_match_index_backwards() -> None:
    """★ matchIndex is knowledge, and knowledge does not decrease.

    Replies arrive late, out of order and duplicated. Letting an old one lower
    matchIndex un-commits entries the leader has already told a client are safe.
    """
    world = leader_with(2, 2, 2)
    world.node.on_append_entries_reply(2, AppendEntriesReply(term=2, success=True, match_index=3))
    world.node.on_append_entries_reply(2, AppendEntriesReply(term=2, success=True, match_index=1))
    assert world.node.match_index[2] == 3


def test_failure_walks_next_index_back_and_retries() -> None:
    world = leader_with(2, 2, 2)
    before = world.node.next_index[2]
    world.node.on_append_entries_reply(2, AppendEntriesReply(term=2, success=False))

    assert world.node.next_index[2] < before
    assert any(isinstance(m, AppendEntries) for m in world.sent_to(2)), "no retry was sent"


def test_next_index_never_goes_below_one() -> None:
    world = leader_with(2)
    for _ in range(10):
        world.node.on_append_entries_reply(2, AppendEntriesReply(term=2, success=False))
    assert world.node.next_index[2] >= 1


def test_a_higher_term_in_a_reply_deposes_the_leader() -> None:
    """★ Not a log disagreement -- an election happened without us. Retrying lower would
    be a deposed leader still trying to replicate."""
    world = leader_with(2, 2)
    world.node.on_append_entries_reply(2, AppendEntriesReply(term=9, success=False))
    assert world.node.role is Role.FOLLOWER
    assert world.node.current_term == 9


def test_replies_are_ignored_once_we_are_not_the_leader() -> None:
    world = Harness(term=2, log_terms=(2, 2))
    world.node.on_append_entries_reply(2, AppendEntriesReply(term=2, success=True, match_index=2))
    assert world.node.match_index.get(2, 0) == 0


# =====================================================================================
# advance_commit_index  --  §5.4.2
# =====================================================================================


def test_a_majority_commits_an_entry_from_the_current_term() -> None:
    world = leader_with(2, 2, term=2)
    world.node.match_index[2] = 2
    world.node.advance_commit_index()
    assert world.node.commit_index == 2


def test_one_follower_out_of_two_is_a_majority_with_the_leader() -> None:
    world = leader_with(2, term=2)
    world.node.match_index[2] = 1
    world.node.match_index[3] = 0
    world.node.advance_commit_index()
    assert world.node.commit_index == 1


def test_a_minority_commits_nothing() -> None:
    world = Harness(term=2, log_terms=(2, 2, 2), peers=(2, 3, 4, 5))
    world.node.become_leader()
    world.node.match_index[2] = 3
    world.node.advance_commit_index()
    assert world.node.commit_index == 0


def test_an_entry_from_an_earlier_term_is_not_committed_by_replica_count() -> None:
    """★★ §5.4.2 and Figure 8 -- the subtlest rule in the paper.

    Our log holds one entry from term 1. We are leader in term 3 and both followers have
    it. That is a majority, and it is *still not committed*: a future leader can
    overwrite it. Counting replicas is not sufficient.
    """
    world = leader_with(1, term=3)
    world.node.match_index[2] = 1
    world.node.match_index[3] = 1
    world.node.advance_commit_index()
    assert world.node.commit_index == 0, "committed an old-term entry on replica count alone"


def test_an_old_entry_commits_indirectly_behind_a_current_one() -> None:
    """★ The other half of §5.4.2: once an entry from the current term commits,
    everything before it commits with it."""
    world = leader_with(1, 3, term=3)
    world.node.match_index[2] = 2
    world.node.match_index[3] = 2
    world.node.advance_commit_index()
    assert world.node.commit_index == 2


def test_commit_index_never_decreases() -> None:
    world = leader_with(3, 3, 3, term=3)
    world.node.match_index[2] = 3
    world.node.match_index[3] = 3
    world.node.advance_commit_index()
    world.node.match_index[2] = 1
    world.node.match_index[3] = 1
    world.node.advance_commit_index()
    assert world.node.commit_index == 3


def test_committing_applies_to_the_state_machine() -> None:
    world = Harness(term=2)
    world.node.log = RaftLog([LogEntry(term=2, index=1, command=Command.put("x", "9"))])
    world.node.become_leader()
    world.node.match_index[2] = 1
    world.node.advance_commit_index()
    assert world.node.kv.get("x") == "9"


# =====================================================================================
# on_client_request
# =====================================================================================


def test_a_follower_redirects_the_client() -> None:
    world = Harness(term=2)
    world.node.leader_id = 3
    world.node.on_client_request(0, ClientRequest(request_id=1, command=Command.put("x", "1")))

    reply = world.only_reply()
    assert isinstance(reply, ClientReply)
    assert not reply.ok
    assert reply.leader_hint == 3


def test_a_leader_takes_the_write_but_does_not_answer_yet() -> None:
    """★ The whole difference from rung 3. The client is told nothing until the entry is
    committed, because until then it is not safe and saying so would be a lie."""
    world = leader_with(term=2)
    world.node.on_client_request(0, ClientRequest(request_id=1, command=Command.put("x", "1")))

    assert world.node.log.last_index() == 1
    assert world.node.log.entry_at(1).term == 2
    assert not [m for _, m in world.net.sent if isinstance(m, ClientReply)]


def test_the_client_hears_back_once_the_entry_commits() -> None:
    world = leader_with(term=2)
    world.node.on_client_request(0, ClientRequest(request_id=7, command=Command.put("x", "1")))
    world.node.on_append_entries_reply(2, AppendEntriesReply(term=2, success=True, match_index=1))

    replies = [m for _, m in world.net.sent if isinstance(m, ClientReply)]
    assert replies, "the entry committed and nobody told the client"
    assert replies[-1].request_id == 7
    assert replies[-1].ok


def test_the_write_reaches_the_followers() -> None:
    world = leader_with(term=2)
    world.node.on_client_request(0, ClientRequest(request_id=1, command=Command.put("x", "1")))
    for peer in (2, 3):
        appends = [m for m in world.sent_to(peer) if isinstance(m, AppendEntries) and m.entries]
        assert appends, f"peer {peer} was never sent the entry"
