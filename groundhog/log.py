"""The replicated log: the thing all of Raft exists to agree about.

Indexing, stated once so it never has to be guessed
---------------------------------------------------
**The log is 1-based.** The first entry is at index 1. Index 0 is a sentinel meaning
"before the beginning": it always exists, its term is 0, and every log matches every
other log at index 0.

That sounds like a fussy detail and it is the opposite. The AppendEntries consistency
check asks "do you have an entry at prevLogIndex with term prevLogTerm?", and for the
very first entry the leader sends, prevLogIndex is 0. If index 0 were not defined to
match, replication could never start. Half of Raft's off-by-one bugs are this sentinel.

`term_at(0)` is 0. `has_entry(0)` is True. Nothing is stored there.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Final

from groundhog.kv import Command
from groundhog.types import Index, Term

#: The index before the first real entry. Always matches, always term 0.
SENTINEL_INDEX: Final[Index] = 0
SENTINEL_TERM: Final[Term] = 0


@dataclass(frozen=True, slots=True)
class LogEntry:
    #: The term of the **leader that created this entry** -- not the term it was
    #: replicated in, and not the current term. This is what makes the commit rule in
    #: §5.4.2 expressible at all.
    term: Term
    index: Index
    command: Command

    def describe(self) -> str:
        return f"{self.index}@{self.term}:{self.command.describe()}"


class RaftLog:
    """An append-only list of entries, addressed from 1."""

    __slots__ = ("_entries",)

    def __init__(self, entries: Iterable[LogEntry] = ()) -> None:
        self._entries: list[LogEntry] = list(entries)
        self._check_contiguous()

    # -- reading --------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        return f"RaftLog({[entry.describe() for entry in self._entries]})"

    def last_index(self) -> Index:
        return self._entries[-1].index if self._entries else SENTINEL_INDEX

    def last_term(self) -> Term:
        return self._entries[-1].term if self._entries else SENTINEL_TERM

    def has_entry(self, index: Index) -> bool:
        """True if `index` is a position this log can vouch for.

        Index 0 counts: every log agrees about the empty prefix.
        """
        return SENTINEL_INDEX <= index <= self.last_index()

    def term_at(self, index: Index) -> Term:
        """The term of the entry at `index`. 0 at the sentinel.

        Raises `IndexError` past the end -- on purpose. Figure 2's AppendEntries rule 2
        says to *reply false* when the log does not contain an entry at prevLogIndex,
        so the containment check belongs in the handler where the reply is written, not
        hidden behind a helper that quietly returns 0.
        """
        if index == SENTINEL_INDEX:
            return SENTINEL_TERM
        return self.entry_at(index).term

    def entry_at(self, index: Index) -> LogEntry:
        if not 1 <= index <= self.last_index():
            raise IndexError(f"no entry at index {index}; log ends at {self.last_index()}")
        return self._entries[index - 1]

    def slice_from(self, index: Index) -> list[LogEntry]:
        """Every entry from `index` onwards. Empty if there are none."""
        if index <= SENTINEL_INDEX:
            raise IndexError(f"slice_from({index}): the log starts at 1")
        return self._entries[index - 1 :]

    def entries(self) -> list[LogEntry]:
        return list(self._entries)

    # -- writing --------------------------------------------------------------

    def append(self, entries: Sequence[LogEntry]) -> None:
        """Add entries to the end. They must continue from `last_index()`."""
        for entry in entries:
            expected = self.last_index() + 1
            if entry.index != expected:
                raise ValueError(f"entry index {entry.index} does not follow {expected - 1}")
            self._entries.append(entry)

    def truncate_from(self, index: Index) -> None:
        """Delete the entry at `index` and everything after it.

        `truncate_from(1)` empties the log. Truncating past the end does nothing, which
        matters: a follower whose log is already shorter than the conflict point is not
        an error case.
        """
        if index <= SENTINEL_INDEX:
            raise IndexError(f"truncate_from({index}): the log starts at 1")
        del self._entries[index - 1 :]

    def create(self, term: Term, command: Command) -> LogEntry:
        """Build the next entry for this log without appending it."""
        return LogEntry(term=term, index=self.last_index() + 1, command=command)

    # -- internals ------------------------------------------------------------

    def _check_contiguous(self) -> None:
        for position, entry in enumerate(self._entries, start=1):
            if entry.index != position:
                raise ValueError(
                    f"log is not contiguous: entry {position} claims index {entry.index}"
                )
