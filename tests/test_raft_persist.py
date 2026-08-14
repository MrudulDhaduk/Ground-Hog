"""Raft's persistent state, and what a crash is allowed to take.

The rule under test throughout: *anything that was synced comes back*. A node that
forgets a vote it granted can grant a second one in the same term, and two leaders in
one term is where every safety proof in the paper stops working.
"""

import pytest

from groundhog.kv import Command
from groundhog.log import LogEntry, RaftLog
from groundhog.raft.persist import HardState, RaftStorage, replay
from groundhog.sim.disk import DiskFaults, SimStorage
from groundhog.sim.rng import Rng


def entry(index: int, term: int, value: str = "v") -> LogEntry:
    return LogEntry(term=term, index=index, command=Command.put(f"k{index}", value))


def fresh(seed: int = 1, **faults: object) -> tuple[RaftStorage, SimStorage]:
    disk = SimStorage(Rng(seed), faults=DiskFaults(**faults))  # type: ignore[arg-type]
    return RaftStorage(disk), disk


def test_a_fresh_node_starts_at_term_zero_with_no_vote() -> None:
    storage, _ = fresh()
    state, log = storage.recover()
    assert state == HardState(current_term=0, voted_for=None)
    assert len(log) == 0


def test_hard_state_survives_a_sync_and_a_crash() -> None:
    storage, disk = fresh()
    storage.save_state(HardState(current_term=7, voted_for=2))
    storage.sync()

    disk.crash()
    disk.restart()
    state, _ = storage.recover()
    assert state == HardState(current_term=7, voted_for=2)


def test_the_latest_state_record_wins() -> None:
    storage, _ = fresh()
    storage.save_state(HardState(current_term=1, voted_for=None))
    storage.save_state(HardState(current_term=2, voted_for=3))
    storage.save_state(HardState(current_term=2, voted_for=1))
    state, _ = storage.recover()
    assert state == HardState(current_term=2, voted_for=1)


def test_a_null_vote_round_trips() -> None:
    """`voted_for` is nullable and node ids start at 1, so 0 stands in for null."""
    storage, _ = fresh()
    storage.save_state(HardState(current_term=4, voted_for=None))
    state, _ = storage.recover()
    assert state.voted_for is None


def test_entries_come_back_in_order() -> None:
    storage, _ = fresh()
    entries = [entry(1, 1), entry(2, 1), entry(3, 2)]
    storage.append_entries(entries)
    storage.sync()

    _, log = storage.recover()
    assert log.entries() == entries
    assert log.last_index() == 3
    assert log.last_term() == 2


def test_state_and_entries_share_one_log_without_confusing_each_other() -> None:
    storage, _ = fresh()
    storage.save_state(HardState(current_term=1, voted_for=None))
    storage.append_entries([entry(1, 1)])
    storage.save_state(HardState(current_term=2, voted_for=2))
    storage.append_entries([entry(2, 2)])
    storage.sync()

    state, log = storage.recover()
    assert state == HardState(current_term=2, voted_for=2)
    assert [e.index for e in log.entries()] == [1, 2]


def test_a_truncation_is_replayed_not_rewritten() -> None:
    """Truncation is an appended record, so the recovery path never has to cope with a
    physically rewritten file."""
    storage, _ = fresh()
    storage.append_entries([entry(1, 1), entry(2, 1), entry(3, 1)])
    storage.truncate_from(2)
    storage.append_entries([entry(2, 5)])
    storage.sync()

    _, log = storage.recover()
    assert [(e.index, e.term) for e in log.entries()] == [(1, 1), (2, 5)]


def test_truncating_everything_leaves_an_empty_log() -> None:
    storage, _ = fresh()
    storage.append_entries([entry(1, 1), entry(2, 1)])
    storage.truncate_from(1)
    storage.sync()
    _, log = storage.recover()
    assert len(log) == 0


def test_unsynced_writes_are_lost_on_a_crash() -> None:
    storage, disk = fresh(lose_unsynced_percent=100)
    storage.save_state(HardState(current_term=9, voted_for=1))
    storage.append_entries([entry(1, 9)])
    # no sync

    disk.crash()
    disk.restart()
    state, log = storage.recover()
    assert state == HardState(current_term=0, voted_for=None)
    assert len(log) == 0


def test_a_vote_that_was_synced_is_never_forgotten() -> None:
    """The reason Figure 2 says 'before responding to RPCs'. 200 crash-prone universes,
    and a synced vote comes back in every one."""
    for seed in range(200):
        storage, disk = fresh(seed, lose_unsynced_percent=50, write_error_percent=0)
        storage.save_state(HardState(current_term=3, voted_for=2))
        storage.sync()
        storage.append_entries([entry(1, 3)])  # deliberately not synced

        disk.crash()
        disk.restart()
        state, _ = storage.recover()
        assert state == HardState(current_term=3, voted_for=2), f"seed {seed} forgot its vote"


def test_recovery_always_yields_a_contiguous_log() -> None:
    """A torn tail must never leave a log with a hole in it -- `RaftLog` would refuse to
    be constructed, which is exactly the check being relied on here."""
    for seed in range(200):
        storage, disk = fresh(seed, lose_unsynced_percent=50)
        storage.append_entries([entry(i, 1) for i in range(1, 6)])
        disk.crash()
        disk.restart()
        _, log = storage.recover()  # RaftLog() raises if the entries are not contiguous
        assert [e.index for e in log.entries()] == list(range(1, len(log) + 1))


def test_replay_rejects_a_record_it_does_not_understand() -> None:
    with pytest.raises(ValueError, match="unknown WAL record kind"):
        replay([b"\x09" + b"\x00" * 16])


def test_an_empty_wal_replays_to_defaults() -> None:
    recovered = replay([])
    assert recovered.state == HardState()
    assert recovered.entries == []
    assert len(RaftLog(recovered.entries)) == 0
