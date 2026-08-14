"""What a Raft node writes down, so that dying does not make it a liar.

Figure 2's persistent state is three things -- `currentTerm`, `votedFor` and the log --
and one sentence governs all of them: *updated on stable storage before responding to
RPCs*. A node that grants a vote and then forgets it can grant a second vote in the same
term, and two leaders in one term is the end of every safety argument in the paper.

The format
----------
One append-only WAL holding three kinds of record:

    STATE     currentTerm, votedFor          -- the latest one wins
    ENTRY     a log entry                    -- appended in order
    TRUNCATE  an index                       -- everything from here was deleted

Recovery replays them in order. Truncation is a *record*, not a physical rewrite of the
file, which matters more than it looks: physically cutting a file that interleaves state
and entries would need the state records moved, and a crash halfway through that is a
new failure mode nobody asked for. Appending "I deleted from index 7" is one more
append, and appends are the operation the M2 disk model already makes safe.

The cost is that the WAL grows forever. Log compaction is explicitly out of scope
(spec §4), so this is a known and accepted leak rather than an oversight.
"""

import struct
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from groundhog.kv import decode_command, encode_command
from groundhog.log import LogEntry, RaftLog
from groundhog.storage import Storage
from groundhog.types import Index, NodeId, Term

_STATE: Final = 1
_ENTRY: Final = 2
_TRUNCATE: Final = 3

#: kind, term, index/voted-for. Big-endian, like everything else on disk.
_HEADER: Final = struct.Struct(">BQQ")

#: `votedFor` is nullable, and 0 is not a valid node id, so it stands in for null.
_NO_VOTE: Final = 0


@dataclass(frozen=True, slots=True)
class HardState:
    """The two scalars that must survive a crash."""

    current_term: Term = 0
    voted_for: NodeId | None = None

    def describe(self) -> str:
        return f"term={self.current_term} voted_for={self.voted_for}"


def encode_state_record(state: HardState) -> bytes:
    voted = _NO_VOTE if state.voted_for is None else state.voted_for
    return _HEADER.pack(_STATE, state.current_term, voted)


def encode_entry_record(entry: LogEntry) -> bytes:
    return _HEADER.pack(_ENTRY, entry.term, entry.index) + encode_command(entry.command)


def encode_truncate_record(index: Index) -> bytes:
    return _HEADER.pack(_TRUNCATE, 0, index)


@dataclass(frozen=True, slots=True)
class Recovered:
    state: HardState
    entries: list[LogEntry]


def replay(records: Sequence[bytes]) -> Recovered:
    """Rebuild hard state and the log from a WAL image.

    Records past a torn tail are already gone -- `Storage.read_all()` only ever returns
    whole, checksummed records -- so this never has to cope with half a record.
    """
    state = HardState()
    entries: list[LogEntry] = []

    for record in records:
        kind, first, second = _HEADER.unpack_from(record, 0)
        payload = record[_HEADER.size :]

        if kind == _STATE:
            state = HardState(
                current_term=first,
                voted_for=None if second == _NO_VOTE else second,
            )
        elif kind == _ENTRY:
            entries.append(LogEntry(term=first, index=second, command=decode_command(payload)))
        elif kind == _TRUNCATE:
            del entries[second - 1 :]
        else:
            raise ValueError(f"unknown WAL record kind: {kind}")

    return Recovered(state=state, entries=entries)


class RaftStorage:
    """A Raft-shaped view of a `Storage`.

    Nothing here decides *when* to sync. That is the whole of the M5 persistence
    exercise: `save_state` and `append_entries` only put bytes in the buffer, and until
    `sync()` returns they are a rumour.
    """

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def save_state(self, state: HardState) -> None:
        self.storage.append([encode_state_record(state)])

    def append_entries(self, entries: Sequence[LogEntry]) -> None:
        if not entries:
            return
        self.storage.append([encode_entry_record(entry) for entry in entries])

    def truncate_from(self, index: Index) -> None:
        self.storage.append([encode_truncate_record(index)])

    def sync(self) -> None:
        """Return only once everything written so far will survive a crash."""
        self.storage.sync()

    def recover(self) -> tuple[HardState, RaftLog]:
        recovered = replay(self.storage.read_all())
        return recovered.state, RaftLog(recovered.entries)
