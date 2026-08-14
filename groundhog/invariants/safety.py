"""The five properties in Figure 3 of the paper, watched continuously.

They form a chain, and the order matters for debugging: when several fire at once, the
earliest one in this list is usually the cause and the rest are consequences.

    Election Safety      at most one leader per term
    Log Matching         same (index, term) => every preceding entry is identical
    Leader Completeness  a committed entry is in every future leader's log
    State Machine Safety nobody applies two different entries at the same index

Leader Append-Only ("a leader never overwrites or deletes its own entries") is the fifth
in Figure 3 and is checked here too, folded into `LogMatching`'s neighbourhood as
`LeaderAppendOnly`, because it is the cheapest early warning of the other four.
"""

from dataclasses import dataclass, field

from groundhog.invariants.base import Checker, ClusterView
from groundhog.invariants.history import AppliedEntry
from groundhog.log import LogEntry
from groundhog.types import Index, NodeId, Term


@dataclass(slots=True)
class ElectionSafety:
    """**At most one leader per term.**

    The one that fails loudest, and usually first. Two leaders in one term means two
    machines both believe they can commit, and every argument in the paper stops
    working from there.

    Nodes claiming leadership in *different* terms is not a violation -- a deposed
    leader stuck behind a partition still thinks it is in charge, which is exactly why
    Raft ties authority to a term rather than to a role.
    """

    name: str = "election_safety"
    leaders: dict[Term, NodeId] = field(default_factory=dict)

    def observe(self, view: ClusterView) -> str | None:
        for node in view.leaders():
            incumbent = self.leaders.get(node.current_term)
            if incumbent is None:
                self.leaders[node.current_term] = node.node_id
            elif incumbent != node.node_id:
                return f"term {node.current_term} has two leaders: n{incumbent} and n{node.node_id}"
        return None


@dataclass(slots=True)
class LogMatching:
    """**If two logs hold an entry with the same index and term, every preceding entry
    is identical.**

    Checked in the efficient direction. If the property holds, the two logs share a
    common prefix and then diverge forever -- so it is enough to find the first index
    where the entries differ and prove no *later* index has matching terms. A matching
    term after a divergence means two different leaders created entries with the same
    term, which cannot happen if Election Safety holds.
    """

    name: str = "log_matching"

    def observe(self, view: ClusterView) -> str | None:
        nodes = view.sorted_nodes()
        for position, left in enumerate(nodes):
            for right in nodes[position + 1 :]:
                shared = min(left.log.last_index(), right.log.last_index())
                if shared == 0:
                    continue

                diverged_at = shared + 1
                for index in range(1, shared + 1):
                    if left.log.entry_at(index) != right.log.entry_at(index):
                        diverged_at = index
                        break

                for index in range(diverged_at, shared + 1):
                    if left.log.term_at(index) != right.log.term_at(index):
                        continue
                    term = left.log.term_at(index)
                    if index == diverged_at:
                        return (
                            f"n{left.node_id} and n{right.node_id} both have term {term} "
                            f"at index {index} but different commands there"
                        )
                    return (
                        f"n{left.node_id} and n{right.node_id} both have term {term} at "
                        f"index {index}, but their logs already differ from index "
                        f"{diverged_at}"
                    )
        return None


@dataclass(slots=True)
class LeaderAppendOnly:
    """**A leader never overwrites or deletes entries in its own log.**

    Cheap, and the earliest warning available: a leader that truncates itself has
    usually accepted an AppendEntries it should have refused, which is the consistency
    check going wrong one step before anything else notices.
    """

    name: str = "leader_append_only"
    #: node -> the log we last saw it holding while it was leader.
    seen: dict[NodeId, list[LogEntry]] = field(default_factory=dict)

    def observe(self, view: ClusterView) -> str | None:
        for node in view.leaders():
            previous = self.seen.get(node.node_id)
            current = node.log.entries()
            if previous is not None and current[: len(previous)] != previous:
                return (
                    f"leader n{node.node_id} in term {node.current_term} changed entries "
                    f"it already had (was {len(previous)} entries)"
                )
            self.seen[node.node_id] = current

        # A node that stops being leader starts over: when it is elected again it may
        # legitimately have been truncated by the leader that replaced it.
        leading: dict[NodeId, None] = dict.fromkeys(node.node_id for node in view.leaders())
        for node_id in list(self.seen):
            if node_id not in leading:
                del self.seen[node_id]
        return None


@dataclass(slots=True)
class LeaderCompleteness:
    """**A committed entry is present in the log of every future leader.**

    This is the one the election restriction (§5.4.1) exists to guarantee, and the one
    that fires when the restriction is missing. The failure is not subtle once you see
    it: a node with a stale log wins an election, and the entry a client was told was
    safe is not in the log of the machine now in charge -- so it gets overwritten.

    "Future" is taken as "elected after we observed the entry committed". Checkers run
    after every event, so the window between an election and noticing it is one event.
    """

    name: str = "leader_completeness"
    committed: dict[Index, LogEntry] = field(default_factory=dict)
    #: term -> already checked, so a long-lived leader is only verified once.
    checked_terms: dict[Term, NodeId] = field(default_factory=dict)

    def observe(self, view: ClusterView) -> str | None:
        for node in view.sorted_nodes():
            for index in range(1, node.commit_index + 1):
                if index not in self.committed and node.log.has_entry(index):
                    self.committed[index] = node.log.entry_at(index)

        for node in view.leaders():
            if self.checked_terms.get(node.current_term) == node.node_id:
                continue
            self.checked_terms[node.current_term] = node.node_id

            for index, entry in sorted(self.committed.items()):
                if not node.log.has_entry(index) or node.log.entry_at(index) != entry:
                    found = (
                        node.log.entry_at(index).describe()
                        if node.log.has_entry(index) and index >= 1
                        else "nothing"
                    )
                    return (
                        f"n{node.node_id} became leader in term {node.current_term} "
                        f"without committed entry {entry.describe()}; it has {found}"
                    )
        return None


@dataclass(slots=True)
class StateMachineSafety:
    """**Once a server applies an entry at an index, no server ever applies a different
    one at that index.**

    The property the outside world can actually feel. Everything above it is the cluster
    agreeing with itself; this is the cluster agreeing with what somebody was promised.

    Note what is deliberately *not* asserted: that every node holds the newest
    acknowledged value. A follower that is merely behind holds an older one, and that is
    lag, not loss. Asserting it would produce a checker that fires on healthy behaviour,
    and a checker that cries wolf gets weakened until it is useless.
    """

    name: str = "state_machine_safety"
    applied: dict[Index, AppliedEntry] = field(default_factory=dict)

    def observe(self, view: ClusterView) -> str | None:
        for node in view.sorted_nodes():
            for index in range(1, node.last_applied + 1):
                if not node.log.has_entry(index):
                    return (
                        f"n{node.node_id} applied up to {node.last_applied} but its log "
                        f"stops at {node.log.last_index()}"
                    )
                entry = node.log.entry_at(index)
                first = self.applied.get(index)
                if first is None:
                    self.applied[index] = AppliedEntry(
                        index=index,
                        term=entry.term,
                        command=entry.command,
                        by=node.node_id,
                    )
                elif (first.term, first.command) != (entry.term, entry.command):
                    return (
                        f"index {index} was applied as {first.describe()}, "
                        f"but n{node.node_id} applied {entry.describe()}"
                    )
        return None


@dataclass(slots=True)
class MonotonicProgress:
    """The bonus checks: `commit_index` never decreases, `last_applied <= commit_index`.

    Neither is a safety property in the paper's sense -- they are the assumptions the
    real ones are written on top of. They earn their place by failing early and
    pointing straight at the line that did it, which the four above do not.

    A crash resets both to zero legitimately, so a restarting node is exempt.
    """

    name: str = "monotonic_progress"
    high_water: dict[NodeId, Index] = field(default_factory=dict)

    def observe(self, view: ClusterView) -> str | None:
        for node in view.sorted_nodes():
            if not node.running:
                self.high_water.pop(node.node_id, None)
                continue

            if node.last_applied > node.commit_index:
                return (
                    f"n{node.node_id} applied {node.last_applied} but has only "
                    f"committed {node.commit_index}"
                )

            previous = self.high_water.get(node.node_id)
            if previous is not None and node.commit_index < previous:
                return (
                    f"n{node.node_id} commit index went backwards: "
                    f"{previous} -> {node.commit_index}"
                )
            self.high_water[node.node_id] = node.commit_index
        return None


def all_checkers() -> list[Checker]:
    """Fresh instances, in the order that makes a multi-violation report readable:
    causes before consequences."""
    return [
        ElectionSafety(),
        LeaderAppendOnly(),
        LogMatching(),
        LeaderCompleteness(),
        StateMachineSafety(),
        MonotonicProgress(),
    ]
