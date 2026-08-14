"""The wire format. Figure 2 of the paper, as frozen dataclasses.

Frozen because a message that can be mutated after it is sent is a message two nodes can
disagree about, and because the network duplicates things -- a duplicate that shares
mutable state with its original is a bug waiting to be blamed on consensus.

There is no `sender` field on any of these. The network already knows who sent a
message, and a node that could put its own name in a message could put someone else's.
"""

from dataclasses import dataclass, field
from typing import TypeAlias

from groundhog.kv import Command
from groundhog.log import LogEntry
from groundhog.types import Index, NodeId, Term


@dataclass(frozen=True, slots=True)
class RequestVote:
    """§5.2 / §5.4.1. `last_log_index` and `last_log_term` exist for one reason: they
    let the receiver refuse a candidate whose log is behind. That refusal is the
    election restriction, and it is the single rule holding up leader completeness."""

    term: Term
    candidate_id: NodeId
    last_log_index: Index
    last_log_term: Term

    def describe(self) -> str:
        return (
            f"RequestVote(t{self.term} from n{self.candidate_id} "
            f"log={self.last_log_index}@{self.last_log_term})"
        )


@dataclass(frozen=True, slots=True)
class RequestVoteReply:
    term: Term
    vote_granted: bool

    def describe(self) -> str:
        return f"RequestVoteReply(t{self.term} granted={self.vote_granted})"


@dataclass(frozen=True, slots=True)
class AppendEntries:
    """§5.3. Also the heartbeat: an AppendEntries with no entries is how a leader says
    "still here", which is why followers reset their election timer on receiving one."""

    term: Term
    leader_id: NodeId
    prev_log_index: Index
    prev_log_term: Term
    entries: tuple[LogEntry, ...] = ()
    leader_commit: Index = 0

    def describe(self) -> str:
        return (
            f"AppendEntries(t{self.term} from n{self.leader_id} "
            f"prev={self.prev_log_index}@{self.prev_log_term} "
            f"n={len(self.entries)} commit={self.leader_commit})"
        )


@dataclass(frozen=True, slots=True)
class AppendEntriesReply:
    """Figure 2 lists only `term` and `success`. `match_index` is an addition, and worth
    understanding rather than accepting:

    With only `success`, a leader must work out *what* succeeded by remembering what it
    last sent to that follower. Replies can arrive late, out of order and duplicated, so
    that memory is stale exactly when it matters, and `matchIndex` walks backwards. Most
    real implementations carry the index in the reply for precisely this reason.

    It does not make the leader's job free. A stale reply still carries a stale
    `match_index`, and `matchIndex` is documented as only ever increasing -- so the
    leader still has to refuse to move it backwards. The bug is still reachable; it is
    just no longer unavoidable.
    """

    term: Term
    success: bool
    #: The follower's highest index that now matches the leader. Meaningless if
    #: `success` is False.
    match_index: Index = 0

    def describe(self) -> str:
        return f"AppendEntriesReply(t{self.term} ok={self.success} match={self.match_index})"


@dataclass(frozen=True, slots=True)
class ClientRequest:
    """A write, from outside the cluster.

    The reply comes back *after the entry is committed*, not when it is accepted. That
    delay is the entire difference between this and rung 3, where the primary said
    "saved" while exactly one machine had the data.
    """

    request_id: int
    command: Command

    def describe(self) -> str:
        return f"ClientRequest(#{self.request_id} {self.command.describe()})"


@dataclass(frozen=True, slots=True)
class ClientReply:
    request_id: int
    ok: bool
    #: Set when a non-leader refuses, so the client knows where to go next. A hint, not
    #: a promise -- by the time it arrives the leader may have changed again.
    leader_hint: NodeId | None = None
    value: str | None = field(default=None)

    def describe(self) -> str:
        where = f" leader=n{self.leader_hint}" if self.leader_hint is not None else ""
        return f"ClientReply(#{self.request_id} ok={self.ok}{where})"


#: Everything that can travel over the network in the Raft world.
Message: TypeAlias = (
    RequestVote
    | RequestVoteReply
    | AppendEntries
    | AppendEntriesReply
    | ClientRequest
    | ClientReply
)
