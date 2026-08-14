"""A Raft node.

**The consensus in this file was written by Claude**, at the user's direction, following
spec §5's suggestion:

    "deliberately let Claude draft a Raft implementation and point your own fault
    injector at it. It will produce code that looks correct and fails on subtle
    orderings -- exactly as humans do. Debugging a wrong Raft with a tool that hands you
    a reproducible seed is the fastest possible way to understand the algorithm."

So read this the way you would read a colleague's pull request, not the way you would
read a reference. It is a genuine best effort against Figure 2 and it has not been
tuned against a sweep. Every claim it makes is checkable: [figure2.md](figure2.md) lists
every rule and the function that must enforce it, and M6's checkers will tell you when
one of them is not true.

What was already plumbing (no consensus insight)
------------------------------------------------
- state fields, split persistent / volatile / leader-only exactly as Figure 2 splits them
- the message dispatch switch
- role transitions: `become_follower`, `become_candidate`, `become_leader`
- timers, including the randomised election timeout draw
- `_apply_committed()` -- the "all servers" apply rule
- `_persist()`, `crash()`, `restart()`, `send()`, `broadcast()`
- `replicate_to()`, which builds a correctly-shaped AppendEntries for one follower

One rule is deliberately **not** handled in the dispatcher, although it would have been
easy: *if any RPC carries a term higher than yours, step down*. Each of the four
handlers does it for itself, which means each of them is a place it could have been
forgotten. Check all four.

Where to look first
-------------------
The four things this implementation could plausibly have got wrong, in the order they
would be worth checking:

- the AppendEntries conflict rule, which must truncate on a *conflict* and not on every
  message;
- `match_index` moving backwards on a late or duplicated reply;
- the §5.4.2 commit rule, which the tests exercise but a sweep exercises harder;
- where each `sync()` sits relative to the reply that depends on it.

    groundhog raft --seed 4471 --faults aggressive --trace out.jsonl
    groundhog raft --scan 0:2000 --faults aggressive
"""

import enum
from collections.abc import Callable, Sequence
from functools import partial
from typing import Final

from groundhog.clock import Clock, Timer
from groundhog.kv import KvStore
from groundhog.log import SENTINEL_INDEX, LogEntry, RaftLog
from groundhog.messages import (
    AppendEntries,
    AppendEntriesReply,
    ClientReply,
    ClientRequest,
    Message,
    RequestVote,
    RequestVoteReply,
)
from groundhog.network import Network
from groundhog.raft.persist import HardState, RaftStorage
from groundhog.sim.rng import Rng
from groundhog.sim.trace import Trace
from groundhog.storage import DiskError
from groundhog.types import MILLISECOND, Index, JsonValue, NodeId, Term, Tick

IMPLEMENTED = True

#: §5.2: the timeout is drawn fresh from this range every time the timer is armed.
#: Fixed timeouts split the vote forever; randomised ones do not.
ELECTION_TIMEOUT: Final[tuple[Tick, Tick]] = (150 * MILLISECOND, 300 * MILLISECOND)

#: Comfortably shorter than the lower bound above, or followers time out on a healthy
#: leader and the cluster elects its way to a standstill.
HEARTBEAT_INTERVAL: Final[Tick] = 50 * MILLISECOND


class Role(enum.Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(
        self,
        node_id: NodeId,
        peers: tuple[NodeId, ...],
        clock: Clock,
        net: Network[Message],
        storage: RaftStorage,
        rng: Rng,
        trace: Trace,
        on_failure: Callable[[NodeId], None] | None = None,
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.clock = clock
        self.net = net
        self.storage = storage
        self.rng = rng
        self.trace = trace
        #: Called when the node kills itself because its disk failed. Whoever is
        #: supervising has to crash the disk and arrange a restart; a process that dies
        #: of its own accord still needs somebody to notice.
        self.on_failure = on_failure

        # -- persistent (Figure 2). Must be durable before any dependent reply leaves.
        self.current_term: Term = 0
        self.voted_for: NodeId | None = None
        self.log = RaftLog()

        # -- volatile on all servers
        self.role = Role.FOLLOWER
        self.commit_index: Index = SENTINEL_INDEX
        self.last_applied: Index = SENTINEL_INDEX
        self.kv = KvStore()

        # -- volatile on candidates
        self.votes_granted: dict[NodeId, None] = {}

        # -- volatile on leaders, reinitialised on every election
        self.next_index: dict[NodeId, Index] = {}
        self.match_index: dict[NodeId, Index] = {}

        # -- volatile bookkeeping that is not in Figure 2
        self.leader_id: NodeId | None = None
        self.running = True
        self.election_timer: Timer | None = None
        self.heartbeat_timer: Timer | None = None
        #: request_id -> (client node, log index) for replies owed once committed.
        self.pending_clients: dict[Index, tuple[NodeId, int]] = {}

    def __repr__(self) -> str:
        return (
            f"RaftNode(n{self.node_id} {self.role.value} term={self.current_term} "
            f"log={self.log.last_index()} commit={self.commit_index})"
        )

    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1

    @property
    def majority(self) -> int:
        """How many nodes make a majority, this node included."""
        return self.cluster_size // 2 + 1

    # -- lifecycle ------------------------------------------------------------

    def start(self) -> None:
        self._arm_election_timer()

    def crash(self) -> None:
        """The process died. Everything in memory goes.

        The *disk* is not touched here: this file must run unchanged against a real
        `FileStorage` in M9, and a real process does not get to tell its disk it died.
        Whoever kills the node crashes the disk -- `raft/world.py` in simulation, an
        actual `SIGKILL` in the real world.
        """
        if not self.running:
            return
        self.running = False
        self._cancel_timers()
        self.record("raft.crash", role=self.role.value, term=self.current_term)

        self.role = Role.FOLLOWER
        self.commit_index = SENTINEL_INDEX
        self.last_applied = SENTINEL_INDEX
        self.kv = KvStore()
        self.votes_granted = {}
        self.next_index = {}
        self.match_index = {}
        self.leader_id = None
        self.pending_clients = {}
        self.current_term = 0
        self.voted_for = None
        self.log = RaftLog()

    def restart(self) -> None:
        """Come back from the log, exactly as a fresh process would.

        The disk has already been recovered by whoever restarted us; all this does is
        read what survived.
        """
        state, log = self.storage.recover()
        self.current_term = state.current_term
        self.voted_for = state.voted_for
        self.log = log
        self.running = True
        self.record(
            "raft.restart",
            term=self.current_term,
            voted_for=self.voted_for,
            entries=len(self.log),
        )
        self._arm_election_timer()

    # -- dispatch -------------------------------------------------------------

    def on_message(self, frm: NodeId, msg: Message) -> None:
        if not self.running:
            return
        self.record("raft.recv", frm=frm, msg=msg.describe())
        self.guard(partial(self._dispatch, frm, msg))

    def _dispatch(self, frm: NodeId, msg: Message) -> None:
        if isinstance(msg, RequestVote):
            self.on_request_vote(frm, msg)
        elif isinstance(msg, RequestVoteReply):
            self.on_request_vote_reply(frm, msg)
        elif isinstance(msg, AppendEntries):
            self.on_append_entries(frm, msg)
        elif isinstance(msg, AppendEntriesReply):
            self.on_append_entries_reply(frm, msg)
        elif isinstance(msg, ClientRequest):
            self.on_client_request(frm, msg)
        elif isinstance(msg, ClientReply):
            pass  # nodes do not receive these

    def guard(self, action: Callable[[], None]) -> None:
        """Run `action`, and die properly if the disk does.

        M2's contract: a failed write is fail-stop. The node has no idea how much of it
        landed, so the only honest response is to crash and recover from the log. That
        is a plumbing concern, not a consensus one, which is why it is handled here and
        not left in the six functions below -- a `DiskError` escaping a handler would
        otherwise take the whole simulation down.
        """
        try:
            action()
        except DiskError as exc:
            self.record("raft.disk_error", error=str(exc))
            self.crash()
            if self.on_failure is not None:
                self.on_failure(self.node_id)

    # -- yours from here ------------------------------------------------------

    def on_request_vote(self, frm: NodeId, msg: RequestVote) -> None:
        """Decide whether to vote for a candidate, and reply.

        Figure 2, RequestVote receiver, plus the all-servers term rule and §5.4.1.
        """
        # All-servers rule: a term above ours means an election happened without us.
        if msg.term > self.current_term:
            self.become_follower(msg.term)

        if msg.term < self.current_term:
            self._reply_vote(frm, granted=False)
            return

        # §5.4.1, the election restriction. Term of the last entry first, length only
        # as the tie-break -- a longer log ending in an older term is behind, not ahead.
        ours = (self.log.last_term(), self.log.last_index())
        theirs = (msg.last_log_term, msg.last_log_index)
        up_to_date = theirs >= ours

        granted = self.voted_for in (None, msg.candidate_id) and up_to_date
        if granted:
            self.voted_for = msg.candidate_id
            # Granting a vote is an admission that somebody else may be in charge, so
            # do not go starting an election of our own for another full timeout.
            self._arm_election_timer()

        self._reply_vote(frm, granted=granted)

    def _reply_vote(self, frm: NodeId, *, granted: bool) -> None:
        """Persist first, then answer. The reply is a promise about durable state."""
        self.persist()
        self.storage.sync()
        self.send(frm, RequestVoteReply(term=self.current_term, vote_granted=granted))

    def on_request_vote_reply(self, frm: NodeId, msg: RequestVoteReply) -> None:
        """Count a vote, and take charge on a majority."""
        if msg.term > self.current_term:
            self.become_follower(msg.term)
            self.persist()
            self.storage.sync()
            return

        # A reply from an older term answers a question we stopped asking.
        if self.role is not Role.CANDIDATE or msg.term != self.current_term:
            return
        if not msg.vote_granted:
            return

        # Keyed by voter, so a duplicated reply is not a second vote.
        self.votes_granted[frm] = None
        if len(self.votes_granted) >= self.majority:
            self.become_leader()

    def on_append_entries(self, frm: NodeId, msg: AppendEntries) -> None:
        """Take entries from a leader, or refuse and say why."""
        if msg.term > self.current_term:
            self.become_follower(msg.term, leader_id=msg.leader_id)

        if msg.term < self.current_term:
            # Rule 1. Do not touch the election timer: a stale leader is not evidence
            # that anybody is in charge.
            self.send(frm, AppendEntriesReply(term=self.current_term, success=False))
            return

        # Same term: this is the legitimate leader, so a candidate gives up here.
        self.become_follower(msg.term, leader_id=msg.leader_id)
        self._arm_election_timer()

        # Rule 2, the consistency check.
        if not self.log.has_entry(msg.prev_log_index) or (
            self.log.term_at(msg.prev_log_index) != msg.prev_log_term
        ):
            self.persist()
            self.storage.sync()
            self.send(frm, AppendEntriesReply(term=self.current_term, success=False))
            return

        # Rules 3 and 4. Truncate only where an entry actually conflicts -- a
        # retransmission of entries we already hold must leave the log alone, or a
        # duplicated message would delete committed entries.
        appended: list[LogEntry] = []
        for offset, incoming in enumerate(msg.entries):
            index = msg.prev_log_index + 1 + offset
            if index <= self.log.last_index():
                if self.log.term_at(index) == incoming.term:
                    continue
                self.log.truncate_from(index)
                self.persist_truncation(index)
            self.log.append([incoming])
            appended.append(incoming)

        self.persist_entries(appended)
        self.persist()
        self.storage.sync()

        # Rule 5. Never past the last entry this message actually delivered: a leader
        # with commit index 9 must not make a follower holding 3 entries claim 9.
        last_new = msg.prev_log_index + len(msg.entries)
        if msg.leader_commit > self.commit_index:
            self.commit_index = min(msg.leader_commit, last_new)
            self._apply_committed()

        self.send(
            frm,
            AppendEntriesReply(term=self.current_term, success=True, match_index=last_new),
        )

    def on_append_entries_reply(self, frm: NodeId, msg: AppendEntriesReply) -> None:
        """Advance or walk back what we believe about one follower."""
        if msg.term > self.current_term:
            # Not a log disagreement -- an election happened without us.
            self.become_follower(msg.term)
            self.persist()
            self.storage.sync()
            return

        if self.role is not Role.LEADER or msg.term != self.current_term:
            return

        if msg.success:
            # `max` because replies arrive late, duplicated and out of order, and
            # matchIndex is knowledge: it does not decrease.
            self.match_index[frm] = max(self.match_index.get(frm, 0), msg.match_index)
            self.next_index[frm] = self.match_index[frm] + 1
            self.advance_commit_index()
            return

        # The follower's log disagrees. Guess lower and try again.
        current = self.next_index.get(frm, self.log.last_index() + 1)
        self.next_index[frm] = max(1, current - 1)
        self.replicate_to(frm)

    def advance_commit_index(self) -> None:
        """Work out how far we may safely commit. **§5.4.2.**"""
        if self.role is not Role.LEADER:
            return

        for candidate in range(self.log.last_index(), self.commit_index, -1):
            # Figure 8: an entry from an earlier term can sit on a majority of servers
            # and still be overwritten by a later leader. Replica count alone is not
            # enough; an old entry only becomes committed indirectly, behind one of
            # ours. Skipping it here is that rule.
            if self.log.term_at(candidate) != self.current_term:
                continue

            # Counting ourselves: our own log is replicated by definition.
            replicas = 1 + sum(
                1 for peer in self.peers if self.match_index.get(peer, 0) >= candidate
            )
            if replicas >= self.majority:
                self.commit_index = candidate
                self._apply_committed()
                return

    def on_election_timeout(self) -> None:
        """Nobody has been in charge for a while. Try to be."""
        self.current_term += 1
        self.become_candidate()
        self.voted_for = self.node_id
        self._arm_election_timer()

        # The term and the self-vote are persistent, and every message below depends on
        # both. Coming back from a crash having forgotten this vote would let us vote
        # again in the same term.
        self.persist()
        self.storage.sync()

        if len(self.votes_granted) >= self.majority:
            self.become_leader()
            return

        self.broadcast(
            RequestVote(
                term=self.current_term,
                candidate_id=self.node_id,
                # Advertising our log is what lets everyone else apply §5.4.1 to us.
                last_log_index=self.log.last_index(),
                last_log_term=self.log.last_term(),
            )
        )

    def on_client_request(self, frm: NodeId, msg: ClientRequest) -> None:
        """Accept a write, if we are the leader -- and say nothing until it is safe."""
        if self.role is not Role.LEADER:
            self.send(
                frm,
                ClientReply(request_id=msg.request_id, ok=False, leader_hint=self.leader_id),
            )
            return

        entry = self.log.create(self.current_term, msg.command)
        self.log.append([entry])
        self.persist_entries([entry])
        self.storage.sync()

        # The debt is recorded, not paid. `_apply_committed()` answers once a majority
        # holds it -- which is the whole difference from rung 3.
        self.pending_clients[entry.index] = (frm, msg.request_id)

        for peer in self.peers:
            self.replicate_to(peer)
        self.advance_commit_index()

    # -- provided: role transitions -------------------------------------------

    def become_follower(self, term: Term, *, leader_id: NodeId | None = None) -> None:
        """Step down (or stay down) at `term`. Clears the vote if the term moved."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        was = self.role
        self.role = Role.FOLLOWER
        self.leader_id = leader_id
        self.votes_granted = {}
        self._cancel_heartbeat()
        if was is not Role.FOLLOWER:
            self.record("raft.step_down", was=was.value, term=self.current_term)

    def become_candidate(self) -> None:
        self.role = Role.CANDIDATE
        self.leader_id = None
        self.votes_granted = {self.node_id: None}
        self.record("raft.candidate", term=self.current_term)

    def become_leader(self) -> None:
        """Take charge, and reinitialise the leader-only state (Figure 2).

        `next_index` starts optimistic -- one past the end of the leader's own log --
        and gets walked back by failed AppendEntries until it finds where each follower
        actually agrees. `match_index` starts pessimistic at 0, because nothing is known
        yet.
        """
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self.next_index = {peer: self.log.last_index() + 1 for peer in self.peers}
        self.match_index = dict.fromkeys(self.peers, SENTINEL_INDEX)
        self._cancel_election_timer()
        self.record("raft.leader", term=self.current_term, log=self.log.last_index())
        self.send_heartbeats()

    # -- provided: replication plumbing ---------------------------------------

    def replicate_to(self, peer: NodeId) -> None:
        """Send whatever `peer` is missing, according to `next_index`."""
        if self.role is not Role.LEADER:
            return
        next_index = self.next_index.get(peer, self.log.last_index() + 1)
        prev_index = next_index - 1
        entries = self.log.slice_from(next_index) if next_index <= self.log.last_index() else []
        self.send(
            peer,
            AppendEntries(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_index,
                prev_log_term=self.log.term_at(prev_index),
                entries=tuple(entries),
                leader_commit=self.commit_index,
            ),
        )

    def send_heartbeats(self) -> None:
        for peer in self.peers:
            self.replicate_to(peer)
        self._arm_heartbeat_timer()

    def send(self, to: NodeId, msg: Message) -> None:
        self.record("raft.send", to=to, msg=msg.describe())
        self.net.send(to, msg)

    def broadcast(self, msg: Message) -> None:
        for peer in self.peers:
            self.send(peer, msg)

    # -- provided: the all-servers apply rule ---------------------------------

    def _apply_committed(self) -> None:
        """Apply everything committed and not yet applied, and pay off client debts."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.entry_at(self.last_applied)
            self.kv.apply(entry.command)
            self.record("raft.apply", index=entry.index, command=entry.command.describe())

            owed = self.pending_clients.pop(entry.index, None)
            if owed is not None:
                client, request_id = owed
                self.send(client, ClientReply(request_id=request_id, ok=True))

    # -- provided: persistence ------------------------------------------------

    def persist(self) -> None:
        """Write `current_term` and `voted_for` down. **Does not sync.**

        Until `self.storage.sync()` returns, this is a rumour. Deciding where the sync
        goes relative to sending a reply is the M5 persistence exercise.
        """
        self.storage.save_state(HardState(self.current_term, self.voted_for))

    def persist_entries(self, entries: Sequence[LogEntry]) -> None:
        self.storage.append_entries(entries)

    def persist_truncation(self, index: Index) -> None:
        self.storage.truncate_from(index)

    # -- provided: timers -----------------------------------------------------

    def _arm_election_timer(self) -> None:
        self._cancel_election_timer()
        self.election_timer = self.clock.after(
            self.rng.between(*ELECTION_TIMEOUT),
            self._fire_election_timeout,
            name="raft.election_timeout",
            actor=self.node_id,
        )

    def _arm_heartbeat_timer(self) -> None:
        self._cancel_heartbeat()
        self.heartbeat_timer = self.clock.after(
            HEARTBEAT_INTERVAL,
            self._fire_heartbeat,
            name="raft.heartbeat",
            actor=self.node_id,
        )

    def _fire_election_timeout(self) -> None:
        self.election_timer = None
        if self.running:
            self.guard(self.on_election_timeout)

    def _fire_heartbeat(self) -> None:
        self.heartbeat_timer = None
        if self.running and self.role is Role.LEADER:
            self.guard(self.send_heartbeats)

    def _cancel_election_timer(self) -> None:
        if self.election_timer is not None:
            self.election_timer.cancel()
            self.election_timer = None

    def _cancel_heartbeat(self) -> None:
        if self.heartbeat_timer is not None:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None

    def _cancel_timers(self) -> None:
        self._cancel_election_timer()
        self._cancel_heartbeat()

    # -- provided: tracing ----------------------------------------------------

    def record(self, kind: str, **fields: JsonValue) -> None:
        entry: dict[str, JsonValue] = {
            "kind": kind,
            "tick": self.clock.now(),
            "node": self.node_id,
        }
        entry.update(fields)
        self.trace.write(entry)
