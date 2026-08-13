"""A disk that lies, loses and tears -- on a schedule you can reproduce.

The fault menu from spec §3 is short because the real one is short: a disk can succeed,
fail, or write only part of what you gave it. Everything below is one of those three.

The model
---------
Two numbers describe the whole thing:

- `_bytes` -- what the node believes it has written.
- `_durable` -- how many leading bytes have actually reached the platter.

`sync()` moves `_durable` to the end. A crash keeps `_bytes[:_durable]` plus *some
prefix* of the rest, because bytes past the last sync are exactly the bytes the
operating system had not committed yet. Cutting that prefix in the middle of a record
is a torn write; cutting it at zero is the plain "unsynced data is gone" case. Both fall
out of the same line of code, which is a good sign the model is the right shape.

What this deliberately does **not** model, stated here so it ends up in DESIGN.md rather
than in a postmortem:

- Real page cache flushes need not be prefix-ordered, so a real crash can leave a hole
  where this one leaves a clean cut.
- A disk that claims `fsync` succeeded while lying about it. Some do. If yours does,
  this simulator cannot see the bug, and no amount of seeds will change that.
- Truncation is applied immediately and treated as durable at that moment. A real
  truncate is not durable until the next sync, so a crash could bring back records this
  model has already dropped.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from groundhog.codec import Scan, decode_records, encode_records
from groundhog.sim.rng import Rng
from groundhog.storage import DiskError
from groundhog.types import MILLISECOND, Tick


@dataclass(frozen=True, slots=True)
class DiskFaults:
    """How badly the disk behaves. Percentages are per operation."""

    #: `append()` fails, having possibly written part of the data first.
    write_error_percent: int = 0
    #: On a crash, unsynced bytes are lost outright. Otherwise a random prefix of them
    #: survives -- which is where torn records come from.
    lose_unsynced_percent: int = 100
    #: Virtual ticks an `fsync` costs, drawn uniformly from this inclusive range.
    sync_cost: tuple[Tick, Tick] = (0, 0)

    @classmethod
    def aggressive(cls) -> "DiskFaults":
        return cls(
            write_error_percent=2,
            lose_unsynced_percent=50,
            sync_cost=(1 * MILLISECOND, 20 * MILLISECOND),
        )


_EMPTY_SCAN: Final = decode_records(b"")


class SimStorage:
    """A `Storage` whose failures are a function of the seed.

    Also grows `crash()` and `restart()`, which the real one cannot have: a real process
    does not get to decide it has died.
    """

    def __init__(self, rng: Rng, *, faults: DiskFaults | None = None) -> None:
        self.rng = rng
        self.faults = faults if faults is not None else DiskFaults()
        self._bytes = bytearray()
        self._durable = 0
        self._online = True
        self._failed = False
        self._cache: Scan | None = _EMPTY_SCAN

        #: Virtual ticks this disk has charged and nobody has paid yet.
        #:
        #: `sync()` is synchronous -- it returns durable, as the Protocol promises -- so
        #: it cannot block the single-threaded loop by itself. Instead it bills the
        #: caller, who pays by delaying whatever it does next (see `take_owed_ticks`).
        #: The alternative was an async storage interface with completion callbacks;
        #: that is how the industrial simulators do it, and it would make the M5 Raft
        #: code callback-shaped for a fault that a checker can catch directly.
        self.owed_ticks: Tick = 0

    # -- the Storage protocol ------------------------------------------------

    def append(self, records: Sequence[bytes]) -> None:
        self._require_online()
        if not records:
            return
        blob = encode_records(list(records))

        # The draw happens whether or not errors are enabled, so that turning the fault
        # off leaves the rest of the seed's stream where it was.
        if self.rng.chance(self.faults.write_error_percent):
            landed = self.rng.between(0, len(blob))
            self._bytes += blob[:landed]
            self._failed = True
            self._cache = None
            raise DiskError(f"write failed after {landed} of {len(blob)} bytes")

        self._bytes += blob
        self._cache = None

    def sync(self) -> None:
        self._require_online()
        self._durable = len(self._bytes)
        self.owed_ticks += self.rng.between(*self.faults.sync_cost)

    def read_all(self) -> list[bytes]:
        self._require_online()
        return list(self._scan().records)

    def truncate_from(self, index: int) -> None:
        self._require_online()
        scan = self._scan()
        if not 0 <= index <= len(scan.records):
            raise IndexError(f"truncate_from({index}) on a log of {len(scan.records)}")
        offset = scan.offsets[index] if index < len(scan.records) else scan.valid_bytes
        del self._bytes[offset:]
        self._durable = min(self._durable, offset)
        self._cache = None

    def close(self) -> None:
        return None

    # -- things only a simulated disk can do ---------------------------------

    def crash(self) -> int:
        """The node dies. Decide, now, what actually reached the platter.

        Returns how many unsynced bytes survived, which the trace records.
        """
        tail = len(self._bytes) - self._durable
        kept = (
            0 if self.rng.chance(self.faults.lose_unsynced_percent) else self.rng.between(0, tail)
        )
        self.crash_after(kept)
        return kept

    def crash_after(self, kept_unsynced_bytes: int) -> None:
        """Crash with a chosen outcome instead of a drawn one.

        Split out from `crash()` because "what survived" and "how much survived" are
        different questions: tests use this to walk every byte offset exhaustively, and
        M7's shrinker will use it to pin a crash to the one offset that reproduces a
        failure.
        """
        tail = len(self._bytes) - self._durable
        if not 0 <= kept_unsynced_bytes <= tail:
            raise ValueError(f"cannot keep {kept_unsynced_bytes} of {tail} unsynced bytes")
        del self._bytes[self._durable + kept_unsynced_bytes :]
        self._durable = len(self._bytes)
        self._online = False
        self._failed = False
        self._cache = None

    def restart(self) -> None:
        """Recovery: keep the longest valid prefix, throw the torn tail away."""
        scan = decode_records(bytes(self._bytes))
        del self._bytes[scan.valid_bytes :]
        self._durable = len(self._bytes)
        self._online = True
        self._failed = False
        self._cache = scan
        return None

    @property
    def online(self) -> bool:
        return self._online and not self._failed

    def durable_records(self) -> list[bytes]:
        """What a crash right now would leave behind, at minimum.

        Everything synced is here. Whatever else survives is a bonus the caller is not
        allowed to rely on -- which is what the invariants in M6 will check.
        """
        return decode_records(bytes(self._bytes[: self._durable])).records

    def take_owed_ticks(self) -> Tick:
        """Drain the accrued fsync cost. The caller delays its next action by this."""
        owed = self.owed_ticks
        self.owed_ticks = 0
        return owed

    def image(self) -> bytes:
        """The raw byte image. For tests and for the crash-at-every-offset sweep."""
        return bytes(self._bytes)

    # -- internals -----------------------------------------------------------

    def _scan(self) -> Scan:
        if self._cache is None:
            self._cache = decode_records(bytes(self._bytes))
        return self._cache

    def _require_online(self) -> None:
        if not self._online:
            raise DiskError("the node is crashed; restart() before using its disk")
        if self._failed:
            raise DiskError("this disk has failed; the node must crash and recover")
