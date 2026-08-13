"""The simulated disk, and the one promise it must never break.

    Everything that was in the log when `sync()` returned is still there after a crash.

Everything else -- unsynced data, torn tails, failed writes -- is allowed to do whatever
it likes, and this module leans on that.
"""

from typing import Any

import pytest

from groundhog.codec import encode_records
from groundhog.sim.disk import DiskFaults, SimStorage
from groundhog.sim.rng import Rng
from groundhog.storage import DiskError
from groundhog.types import MILLISECOND

PAYLOADS = [b"alpha", b"bravo", b"charlie", b"delta"]


def fresh(seed: int = 1, **faults: Any) -> SimStorage:
    return SimStorage(Rng(seed), faults=DiskFaults(**faults))


def test_unsynced_data_is_lost_on_a_crash() -> None:
    """The important one."""
    disk = fresh(lose_unsynced_percent=100)
    disk.append(PAYLOADS)
    disk.crash()
    disk.restart()
    assert disk.read_all() == []


def test_synced_data_always_survives() -> None:
    disk = fresh(lose_unsynced_percent=100)
    disk.append(PAYLOADS[:2])
    disk.sync()
    disk.append(PAYLOADS[2:])
    disk.crash()
    disk.restart()
    assert disk.read_all() == PAYLOADS[:2]


def test_unsynced_data_sometimes_survives_in_part() -> None:
    """Which is where torn records come from. If this never happened, the recovery
    path would never be exercised by a crash."""
    partial = 0
    for seed in range(50):
        disk = fresh(seed, lose_unsynced_percent=50)
        disk.append(PAYLOADS)
        disk.crash()
        disk.restart()
        recovered = disk.read_all()
        if 0 < len(recovered) < len(PAYLOADS):
            partial += 1
    assert partial > 0


def test_a_torn_record_is_discarded_but_the_ones_before_it_are_not() -> None:
    disk = fresh()
    disk.append(PAYLOADS)
    whole = len(encode_records(PAYLOADS[:2]))
    disk.crash_after(whole + 3)  # two clean records plus a fragment of the third
    disk.restart()
    assert disk.read_all() == PAYLOADS[:2]


def test_crashing_at_every_byte_offset_recovers_a_valid_prefix() -> None:
    """The M2 done-when, exhaustively, on the simulated disk."""
    blob = encode_records(PAYLOADS)
    for kept in range(len(blob) + 1):
        disk = fresh()
        disk.append(PAYLOADS)
        disk.crash_after(kept)
        disk.restart()
        recovered = disk.read_all()
        assert recovered == PAYLOADS[: len(recovered)], f"kept {kept}"
        assert disk.image() == encode_records(recovered), f"kept {kept} not repaired"


def test_crashing_at_every_offset_never_loses_a_synced_record() -> None:
    synced = PAYLOADS[:2]
    unsynced = PAYLOADS[2:]
    tail = len(encode_records(unsynced))
    for kept in range(tail + 1):
        disk = fresh()
        disk.append(synced)
        disk.sync()
        disk.append(unsynced)
        disk.crash_after(kept)
        disk.restart()
        recovered = disk.read_all()
        assert recovered[: len(synced)] == synced, f"kept {kept}"
        assert recovered == PAYLOADS[: len(recovered)]


def test_crash_after_rejects_more_bytes_than_exist() -> None:
    disk = fresh()
    disk.append([b"one"])
    with pytest.raises(ValueError, match="cannot keep"):
        disk.crash_after(1000)


def test_the_disk_survives_a_thousand_random_crashes() -> None:
    """The property, under the aggressive fault profile, across many universes."""
    for seed in range(200):
        rng = Rng(seed)
        disk = SimStorage(rng, faults=DiskFaults.aggressive())
        written: list[bytes] = []
        synced: list[bytes] = []

        for step in range(12):
            record = f"r{step}".encode()
            try:
                disk.append([record])
            except DiskError:
                break
            written.append(record)
            if rng.chance(30):
                disk.sync()
                synced = list(written)

        disk.crash()
        disk.restart()
        recovered = disk.read_all()

        assert recovered == written[: len(recovered)], f"seed {seed}: not a prefix"
        assert len(recovered) >= len(synced), f"seed {seed}: lost a synced record"


# -- failure modes -------------------------------------------------------------------


def test_a_write_error_fails_the_disk_until_it_is_restarted() -> None:
    """Fail-stop: after a failed write the node has no idea how much landed, so the
    only honest thing the disk can do is refuse to be used again."""
    disk = fresh(write_error_percent=100)
    with pytest.raises(DiskError, match="write failed"):
        disk.append([b"nope"])
    with pytest.raises(DiskError, match="has failed"):
        disk.append([b"also nope"])
    with pytest.raises(DiskError, match="has failed"):
        disk.sync()
    with pytest.raises(DiskError, match="has failed"):
        disk.read_all()

    disk.crash()
    disk.restart()
    assert disk.online
    # A failed write is not a write that did not happen: the record may have landed
    # whole, landed torn (and been discarded), or never landed at all. All three are
    # legal, which is exactly why the node must recover rather than assume.
    assert disk.read_all() in ([], [b"nope"])


def test_a_failed_write_may_still_have_left_bytes_behind() -> None:
    """A failed write is not a write that did not happen."""
    partial = 0
    for seed in range(50):
        disk = fresh(seed, write_error_percent=100)
        with pytest.raises(DiskError):
            disk.append([b"a much longer record so a partial write is likely"])
        if len(disk.image()) > 0:
            partial += 1
    assert partial > 0


def test_a_crashed_disk_refuses_everything_until_restart() -> None:
    disk = fresh()
    disk.append([b"one"])
    disk.sync()
    disk.crash()

    for operation in (
        lambda: disk.append([b"two"]),
        disk.sync,
        disk.read_all,
        lambda: disk.truncate_from(0),
    ):
        with pytest.raises(DiskError, match="crashed"):
            operation()

    disk.restart()
    assert disk.read_all() == [b"one"]


def test_restarting_is_idempotent() -> None:
    disk = fresh()
    disk.append(PAYLOADS)
    disk.sync()
    disk.crash()
    disk.restart()
    disk.restart()
    assert disk.read_all() == PAYLOADS


# -- sync cost -----------------------------------------------------------------------


def test_sync_costs_virtual_ticks() -> None:
    disk = fresh(sync_cost=(5 * MILLISECOND, 5 * MILLISECOND))
    disk.append([b"one"])
    disk.sync()
    disk.append([b"two"])
    disk.sync()
    assert disk.owed_ticks == 10 * MILLISECOND
    assert disk.take_owed_ticks() == 10 * MILLISECOND
    assert disk.owed_ticks == 0


def test_a_free_sync_still_consumes_a_draw() -> None:
    """Rule: turning a fault off must not shift the rest of the seed's stream."""
    quiet = fresh(sync_cost=(0, 0))
    quiet.sync()
    assert quiet.rng.calls == 1
    assert quiet.owed_ticks == 0


def test_a_disabled_write_error_still_consumes_a_draw() -> None:
    disk = fresh(write_error_percent=0)
    disk.append([b"one"])
    assert disk.rng.calls == 1


# -- durability introspection --------------------------------------------------------


def test_durable_records_shows_what_a_crash_would_leave() -> None:
    disk = fresh()
    disk.append(PAYLOADS[:2])
    disk.sync()
    disk.append(PAYLOADS[2:])
    assert disk.durable_records() == PAYLOADS[:2]
    assert disk.read_all() == PAYLOADS


def test_truncation_moves_the_durable_point_back() -> None:
    disk = fresh(lose_unsynced_percent=100)
    disk.append(PAYLOADS)
    disk.sync()
    disk.truncate_from(1)
    assert disk.durable_records() == PAYLOADS[:1]
    disk.crash()
    disk.restart()
    assert disk.read_all() == PAYLOADS[:1]


def test_online_reports_the_obvious() -> None:
    # Read into locals rather than asserting on `disk.online` three times: mypy narrows
    # a property access and does not un-narrow it when a method mutates the object, so
    # the second assert would be "provably" false and everything after it unreachable.
    disk = fresh()
    fresh_disk = disk.online
    disk.crash()
    crashed = disk.online
    disk.restart()
    recovered = disk.online
    assert (fresh_disk, crashed, recovered) == (True, False, True)
