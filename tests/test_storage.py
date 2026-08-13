"""The `Storage` contract, run against both implementations.

Every test in the first half runs twice: once on a real file with a real `fsync`, once
on the simulated disk. If the two ever disagree, the abstraction is a lie and M9's claim
-- "the same Raft code runs in both worlds" -- was never true.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest

from groundhog.codec import decode_records, encode_records
from groundhog.sim.disk import SimStorage
from groundhog.sim.rng import Rng
from groundhog.storage import FileStorage, Storage

PAYLOADS = [b"alpha", b"", b"charlie" * 5, b"d"]


@pytest.fixture(params=["file", "sim"])
def storage(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Storage]:
    impl: Storage
    if request.param == "file":
        impl = FileStorage(tmp_path / "wal" / "log.bin")
    else:
        impl = SimStorage(Rng(1))
    yield impl
    impl.close()


def test_a_new_log_is_empty(storage: Storage) -> None:
    assert storage.read_all() == []


def test_appended_records_come_back_in_order(storage: Storage) -> None:
    storage.append(PAYLOADS)
    assert storage.read_all() == PAYLOADS


def test_batches_accumulate(storage: Storage) -> None:
    storage.append([b"one"])
    storage.append([b"two", b"three"])
    storage.append([])
    assert storage.read_all() == [b"one", b"two", b"three"]


def test_appending_nothing_changes_nothing(storage: Storage) -> None:
    storage.append([b"one"])
    storage.append([])
    assert storage.read_all() == [b"one"]


def test_sync_does_not_change_what_is_readable(storage: Storage) -> None:
    storage.append(PAYLOADS)
    before = storage.read_all()
    storage.sync()
    assert storage.read_all() == before


def test_truncate_from_zero_empties_the_log(storage: Storage) -> None:
    storage.append(PAYLOADS)
    storage.truncate_from(0)
    assert storage.read_all() == []


def test_truncate_from_the_end_is_a_no_op(storage: Storage) -> None:
    storage.append(PAYLOADS)
    storage.truncate_from(len(PAYLOADS))
    assert storage.read_all() == PAYLOADS


def test_truncate_from_the_middle_keeps_the_prefix(storage: Storage) -> None:
    storage.append(PAYLOADS)
    storage.truncate_from(2)
    assert storage.read_all() == PAYLOADS[:2]


def test_appending_after_a_truncate_continues_from_there(storage: Storage) -> None:
    """Raft does exactly this when a follower's log conflicts with the leader's."""
    storage.append(PAYLOADS)
    storage.truncate_from(1)
    storage.append([b"replacement"])
    assert storage.read_all() == [PAYLOADS[0], b"replacement"]


@pytest.mark.parametrize("index", [-1, 5])
def test_truncate_out_of_range_is_rejected(storage: Storage, index: int) -> None:
    storage.append(PAYLOADS)
    with pytest.raises(IndexError):
        storage.truncate_from(index)


def test_large_and_empty_records_survive(storage: Storage) -> None:
    records = [b"", b"x" * 100_000, b""]
    storage.append(records)
    storage.sync()
    assert storage.read_all() == records


# -- FileStorage only: it is the one that has to survive a real process dying --------


def test_a_file_log_reopens_with_its_contents(tmp_path: Path) -> None:
    path = tmp_path / "log.bin"
    first = FileStorage(path)
    first.append(PAYLOADS)
    first.sync()
    first.close()

    second = FileStorage(path)
    assert second.read_all() == PAYLOADS
    assert second.discarded_on_open == 0
    second.close()


def test_a_truncation_survives_a_reopen(tmp_path: Path) -> None:
    path = tmp_path / "log.bin"
    first = FileStorage(path)
    first.append(PAYLOADS)
    first.truncate_from(2)
    first.sync()
    first.close()

    second = FileStorage(path)
    assert second.read_all() == PAYLOADS[:2]
    second.close()


def test_opening_a_missing_file_creates_it(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deeper" / "log.bin"
    storage = FileStorage(path)
    storage.close()
    assert path.exists()


def test_reopening_after_a_cut_at_every_byte_offset(tmp_path: Path) -> None:
    """The M2 done-when, on a real file.

    For every possible place a crash could have stopped the write, reopening must give
    back a valid prefix -- and must leave the file repaired, so the next append does not
    land after a corrupt record.
    """
    blob = encode_records(PAYLOADS)
    for cut in range(len(blob) + 1):
        path = tmp_path / f"cut_{cut}.bin"
        path.write_bytes(blob[:cut])

        storage = FileStorage(path)
        recovered = storage.read_all()
        storage.close()

        assert recovered == PAYLOADS[: len(recovered)], f"cut at {cut}"
        assert path.stat().st_size == len(encode_records(recovered)), f"cut at {cut} not repaired"
        assert decode_records(path.read_bytes()).records == recovered


def test_a_repaired_file_can_be_appended_to(tmp_path: Path) -> None:
    path = tmp_path / "log.bin"
    path.write_bytes(encode_records([b"good"]) + encode_records([b"torn"])[:-3])

    storage = FileStorage(path)
    assert storage.read_all() == [b"good"]
    assert storage.discarded_on_open > 0
    storage.append([b"next"])
    storage.sync()
    storage.close()

    reopened = FileStorage(path)
    assert reopened.read_all() == [b"good", b"next"]
    reopened.close()
