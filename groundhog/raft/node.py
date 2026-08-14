"""A Raft node. **The consensus in this file is yours to write.**

Everything below the `-- yours from here --` line raises `NotImplementedError`. Six
functions plus the persistence ordering, from the paper, by hand. That is the entire
point of the project: per spec §5, if this file is written for you and you cannot defend
the election restriction in an interview, the project has negative value.

Read [figure2.md](figure2.md) beside the paper. It lists every rule and which function
has to enforce it.

What is already done (plumbing, no consensus insight)
----------------------------------------------------
- state fields, split persistent / volatile / leader-only exactly as Figure 2 splits them
- the message dispatch switch
- role transitions: `become_follower`, `become_candidate`, `become_leader`
- timers, including the randomised election timeout draw
- `_apply_committed()` -- the "all servers" apply rule
- `_persist()`, `crash()`, `restart()`, `send()`, `broadcast()`
- `replicate_to()`, which builds a correctly-shaped AppendEntries for one follower

One rule is deliberately **not** done for you, although it would have been easy to put in
the dispatcher: *if any RPC carries a term higher than yours, step down*. Forgetting it
in exactly one handler is a classic Raft bug, and it is worth being able to find it here.

When the six functions are written, set `IMPLEMENTED = True` -- `tests/test_raft_node.py`
skips itself until you do.
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

#: Flip this when the six functions below are written.
IMPLEMENTED = False

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

        Figure 2, RequestVote receiver:
          1. reply false if `msg.term < self.current_term` (§5.1)
          2. if `voted_for` is None or already `msg.candidate_id`, **and** the
             candidate's log is at least as up to date as ours, grant it (§5.2, §5.4)

        Plus the all-servers rule: a term higher than yours means you step down first.

        **§5.4.1, the election restriction.** "At least as up to date" is defined by
        comparing the last entries: later term wins; same term, longer log wins. Term
        first, then length. `self.log.last_index()` and `self.log.last_term()` are what
        you compare against.

        Whatever you decide, it is persistent state -- and the reply depends on it.
        """
        raise NotImplementedError("M5 [Y]: see figure2.md")

    def on_request_vote_reply(self, frm: NodeId, msg: RequestVoteReply) -> None:
        """Count a vote, and become leader on a majority.

        Watch for: a reply from an older term answers a question you stopped asking; a
        duplicated reply is not a second vote (`self.votes_granted` is keyed by voter
        for that reason); and a reply carrying a higher term means you lost.

        `self.majority` is the threshold. `become_leader()` handles the state reset.
        """
        raise NotImplementedError("M5 [Y]: see figure2.md")

    def on_append_entries(self, frm: NodeId, msg: AppendEntries) -> None:
        """Take entries from a leader, or refuse and say why.

        Figure 2, AppendEntries receiver:
          1. reply false if `msg.term < self.current_term` (§5.1)
          2. reply false if the log has no entry at `prev_log_index` with term
             `prev_log_term` (§5.3) -- `self.log.has_entry()` and `term_at()`
          3. if an existing entry conflicts with a new one (same index, different term),
             delete it and everything after it (§5.3)
          4. append any new entries not already in the log
          5. if `leader_commit > commit_index`, set
             `commit_index = min(leader_commit, index of last new entry)`

        Rule 3 is narrower than it first reads: delete on *conflict*, not on every
        AppendEntries. Truncating unconditionally throws away committed entries that a
        retransmission just re-sent.

        A valid AppendEntries -- including an empty heartbeat -- is proof of a live
        leader, so `_arm_election_timer()` again. Call `_apply_committed()` when
        `commit_index` moves.

        Reply with `AppendEntriesReply(term, success, match_index)`, where `match_index`
        is your last index that now agrees with the leader.
        """
        raise NotImplementedError("M5 [Y]: see figure2.md")

    def on_append_entries_reply(self, frm: NodeId, msg: AppendEntriesReply) -> None:
        """Advance or walk back what you believe about one follower.

        On success: move `next_index[frm]` and `match_index[frm]` forward. `match_index`
        is knowledge and never goes backwards -- a late or duplicated reply carries a
        stale index, and letting it lower `match_index` un-commits committed entries.

        On failure: work out which kind. A higher term means you are not the leader any
        more. Otherwise the follower's log disagrees, so lower `next_index[frm]` and try
        again with `replicate_to(frm)`.

        A successful reply may make something committable -- `advance_commit_index()`.
        """
        raise NotImplementedError("M5 [Y]: see figure2.md")

    def advance_commit_index(self) -> None:
        """Work out how far a leader may safely commit. **§5.4.2.**

        Find the largest `N > commit_index` such that a majority of nodes have
        `match_index >= N` -- counting yourself, since your own log is replicated by
        definition -- **and** `self.log.term_at(N) == self.current_term`.

        That second condition is the one worth understanding. Figure 8 of the paper
        shows an entry from an earlier term sitting on a majority of servers that is
        still *not* committed and can still be overwritten. Counting replicas is not
        sufficient. An old entry becomes committed only indirectly, when an entry from
        the current term commits after it.

        Move `commit_index`, then `_apply_committed()`.
        """
        raise NotImplementedError("M5 [Y]: see figure2.md")

    def on_election_timeout(self) -> None:
        """Nobody has been in charge for a while. Try to be.

        Figure 2, candidate conversion: increment `current_term`, vote for yourself,
        reset the election timer, send `RequestVote` to every peer.

        The votes you send out advertise your log -- `last_log_index` and
        `last_log_term` -- which is what lets everyone else apply the election
        restriction to you.

        Your new term and self-vote are persistent state, and the RequestVote messages
        depend on both.

        A single-node cluster wins its own election immediately; more usefully, so does
        a cluster where you already hold a majority of one.
        """
        raise NotImplementedError("M5 [Y]: see figure2.md")

    def on_client_request(self, frm: NodeId, msg: ClientRequest) -> None:
        """Accept a write, if you are the leader.

        Not a leader: reply `ok=False` with `leader_hint=self.leader_id`.

        Leader: append the command to your log as an entry in your current term, and
        **do not reply yet**. Record the debt in `pending_clients[index]` and let
        `_apply_committed()` answer once the entry is committed and applied. That delay
        is the whole difference from rung 3.

        `self.log.create(term, command)` builds the entry. `replicate_to(peer)` sends it.
        """
        raise NotImplementedError("M5 [Y]: see figure2.md")

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
