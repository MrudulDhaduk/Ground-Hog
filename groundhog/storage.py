"""Durability, behind an interface.

The second of the three handles from spec §5. A node never opens a file; it appends
records to a `Storage` and calls `sync()`. What that means physically is somebody else's
problem -- an fsync on a real disk, or a decision by the simulator about what survives.

The contract, and it is the only thing Raft is allowed to assume:

    Everything that was in the log when `sync()` **returned** is still there after a
    crash. Everything appended since may or may not be. It is never partially there in
    a way that `read_all()` can see -- a torn tail is discarded on recovery.

`close()` is on the Protocol because `__del__` is banned (rule 5: finaliser order is not
deterministic), so a real handle has to be closed by someone who means it.
"""

import os
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

from groundhog.codec import HEADER_SIZE, decode_records, encode_records


class DiskError(Exception):
    """A write or sync failed.

    Fail-stop. A node that gets one of these has no idea how much of its write landed,
    so the only safe response is to crash and recover from the log, which is exactly
    what a real WAL implementation does.
    """


class Storage(Protocol):
    def append(self, records: Sequence[bytes]) -> None:
        """Add records to the end of the log. Not durable until `sync()` returns."""
        ...

    def sync(self) -> None:
        """Return only once everything appended so far will survive a crash."""
        ...

    def read_all(self) -> list[bytes]:
        """Every record in the log, in order."""
        ...

    def truncate_from(self, index: int) -> None:
        """Discard the record at 0-based position `index` and everything after it.

        `truncate_from(0)` empties the log; `truncate_from(len(read_all()))` does
        nothing. Raft needs this when a follower's log conflicts with the leader's.
        """
        ...

    def close(self) -> None: ...


class FileStorage:
    """The real one. A file, an offset, and `os.fsync`.

    Opening it *is* recovery: the file is scanned, and if the tail does not check out
    it is truncated away before anything else happens. A repaired file is a valid
    prefix of what was written, which is the strongest thing a WAL can promise.

    Honest limitation, worth stating out loud rather than discovering later: this
    fsyncs the file, not the directory that contains it. On POSIX, the *existence* of a
    newly created file is not durable until its parent directory is fsynced too. It
    does not affect the cluster's correctness here because a node that loses its whole
    log looks exactly like a node that never had one, which Raft already handles.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()
        self._file = path.open("r+b")

        scan = decode_records(self._file.read())
        self._records = scan.records
        self._offsets = scan.offsets
        self._end = scan.valid_bytes
        self.discarded_on_open = scan.discarded_bytes
        if scan.is_torn:
            self._file.truncate(self._end)
        self._file.seek(self._end)

    def append(self, records: Sequence[bytes]) -> None:
        if not records:
            return
        blob = encode_records(list(records))
        try:
            self._file.seek(self._end)
            self._file.write(blob)
        except OSError as exc:  # pragma: no cover - real disks fail rarely on demand
            raise DiskError(str(exc)) from exc

        offset = self._end
        for record in records:
            self._records.append(record)
            self._offsets.append(offset)
            offset += HEADER_SIZE + len(record)
        self._end = offset

    def sync(self) -> None:
        try:
            self._file.flush()
            os.fsync(self._file.fileno())
        except OSError as exc:  # pragma: no cover - see above
            raise DiskError(str(exc)) from exc

    def read_all(self) -> list[bytes]:
        return list(self._records)

    def truncate_from(self, index: int) -> None:
        if not 0 <= index <= len(self._records):
            raise IndexError(f"truncate_from({index}) on a log of {len(self._records)}")
        offset = self._offsets[index] if index < len(self._records) else self._end
        self._file.flush()
        self._file.truncate(offset)
        self._file.seek(offset)
        self._end = offset
        del self._records[index:]
        del self._offsets[index:]

    def close(self) -> None:
        self._file.flush()
        self._file.close()
