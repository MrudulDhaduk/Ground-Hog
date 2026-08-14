"""The replicated log, and the sentinel at index 0 that makes replication start."""

import pytest

from groundhog.kv import Command
from groundhog.log import SENTINEL_INDEX, SENTINEL_TERM, LogEntry, RaftLog


def entry(index: int, term: int, value: str = "v") -> LogEntry:
    return LogEntry(term=term, index=index, command=Command.put("k", value))


def log_of(*terms: int) -> RaftLog:
    """A log whose entry at index i has the i-th term given."""
    return RaftLog(entry(index, term) for index, term in enumerate(terms, start=1))


# -- the sentinel ---------------------------------------------------------------------


def test_an_empty_log_ends_at_the_sentinel() -> None:
    empty = RaftLog()
    assert empty.last_index() == SENTINEL_INDEX == 0
    assert empty.last_term() == SENTINEL_TERM == 0
    assert len(empty) == 0


def test_every_log_has_index_zero() -> None:
    """Without this, a leader's first AppendEntries -- which carries prevLogIndex 0 --
    would be refused by everyone and replication could never begin."""
    assert RaftLog().has_entry(0)
    assert log_of(1, 1, 2).has_entry(0)
    assert RaftLog().term_at(0) == 0


def test_negative_indices_are_not_entries() -> None:
    assert not RaftLog().has_entry(-1)


# -- reading --------------------------------------------------------------------------


def test_the_first_entry_is_at_index_one() -> None:
    log = log_of(3)
    assert log.last_index() == 1
    assert log.entry_at(1).term == 3
    assert log.term_at(1) == 3


def test_has_entry_covers_exactly_the_stored_range() -> None:
    log = log_of(1, 1, 2)
    assert [log.has_entry(i) for i in range(5)] == [True, True, True, True, False]


def test_term_at_past_the_end_raises() -> None:
    """Deliberate: Figure 2 says to *reply false* when the log has no entry at
    prevLogIndex, so the containment check belongs in the handler."""
    with pytest.raises(IndexError, match="no entry at index 4"):
        log_of(1, 1, 2).term_at(4)


def test_slice_from_returns_the_tail() -> None:
    log = log_of(1, 1, 2, 2)
    assert [e.index for e in log.slice_from(3)] == [3, 4]
    assert [e.index for e in log.slice_from(1)] == [1, 2, 3, 4]


def test_slicing_past_the_end_is_empty_not_an_error() -> None:
    assert log_of(1, 1).slice_from(3) == []


def test_slicing_from_the_sentinel_is_a_bug() -> None:
    with pytest.raises(IndexError, match="starts at 1"):
        log_of(1).slice_from(0)


def test_last_term_is_the_term_of_the_last_entry_not_the_highest() -> None:
    """They differ after a truncation, and the election restriction compares the last
    entry's term -- not the biggest term the log has ever held."""
    log = log_of(1, 5, 2)
    assert log.last_term() == 2


# -- writing --------------------------------------------------------------------------


def test_append_extends_the_log() -> None:
    log = log_of(1)
    log.append([entry(2, 1), entry(3, 2)])
    assert log.last_index() == 3
    assert log.last_term() == 2


def test_appending_a_gap_is_refused() -> None:
    log = log_of(1)
    with pytest.raises(ValueError, match="does not follow"):
        log.append([entry(3, 1)])


def test_appending_nothing_is_fine() -> None:
    log = log_of(1)
    log.append([])
    assert log.last_index() == 1


def test_truncate_from_deletes_the_entry_and_everything_after() -> None:
    log = log_of(1, 1, 2, 2)
    log.truncate_from(3)
    assert log.last_index() == 2
    assert [e.index for e in log.entries()] == [1, 2]


def test_truncate_from_one_empties_the_log() -> None:
    log = log_of(1, 2, 3)
    log.truncate_from(1)
    assert log.last_index() == 0
    assert len(log) == 0


def test_truncating_past_the_end_does_nothing() -> None:
    """A follower whose log is already shorter than the conflict point is not an error
    case -- it is the common case."""
    log = log_of(1, 1)
    log.truncate_from(9)
    assert log.last_index() == 2


def test_truncating_the_sentinel_is_a_bug() -> None:
    with pytest.raises(IndexError, match="starts at 1"):
        log_of(1).truncate_from(0)


def test_appending_after_a_truncation_continues_correctly() -> None:
    """The conflict-resolution path: cut the divergent tail, then take the leader's."""
    log = log_of(1, 1, 2)
    log.truncate_from(2)
    log.append([entry(2, 5), entry(3, 5)])
    assert [(e.index, e.term) for e in log.entries()] == [(1, 1), (2, 5), (3, 5)]


def test_create_builds_the_next_entry_without_storing_it() -> None:
    log = log_of(1, 1)
    fresh = log.create(term=4, command=Command.put("x", "9"))
    assert (fresh.index, fresh.term) == (3, 4)
    assert log.last_index() == 2


# -- construction ---------------------------------------------------------------------


def test_a_log_built_from_entries_must_be_contiguous() -> None:
    with pytest.raises(ValueError, match="not contiguous"):
        RaftLog([entry(1, 1), entry(3, 1)])


def test_a_log_round_trips_through_its_entries() -> None:
    log = log_of(1, 2, 3)
    assert RaftLog(log.entries()).entries() == log.entries()
