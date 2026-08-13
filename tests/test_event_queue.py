"""Total order in the queue, or nothing below it is reproducible."""

import pytest

from groundhog.sim.event import Event, EventQueue


def noop() -> None:
    return None


def drain(queue: EventQueue) -> list[Event]:
    popped: list[Event] = []
    while (event := queue.pop()) is not None:
        popped.append(event)
    return popped


def test_events_come_out_in_tick_order() -> None:
    queue = EventQueue()
    for tick in (50, 10, 30, 20):
        queue.schedule(tick, kind="t", action=noop)
    assert [e.tick for e in drain(queue)] == [10, 20, 30, 50]


def test_ties_break_by_insertion_order() -> None:
    """The whole reason `seq` exists."""
    queue = EventQueue()
    for name in ("first", "second", "third", "fourth"):
        queue.schedule(100, kind=name, action=noop)
    assert [e.kind for e in drain(queue)] == ["first", "second", "third", "fourth"]


def test_events_are_not_orderable() -> None:
    """The heap must never reach the payload. If it does, this is why it will shout."""
    a = Event(tick=1, seq=0, kind="a", action=noop)
    b = Event(tick=1, seq=1, kind="b", action=noop)
    with pytest.raises(TypeError):
        _ = a < b  # type: ignore[operator]


def test_events_compare_by_identity_only() -> None:
    a = Event(tick=1, seq=0, kind="a", action=noop)
    b = Event(tick=1, seq=0, kind="a", action=noop)
    assert a != b
    assert a == a


def test_cancel_removes_the_event() -> None:
    queue = EventQueue()
    keep = queue.schedule(10, kind="keep", action=noop)
    drop = queue.schedule(20, kind="drop", action=noop)
    assert queue.cancel(drop) is True
    assert [e.kind for e in drain(queue)] == ["keep"]
    assert keep.kind == "keep"


def test_cancelling_twice_is_false_the_second_time() -> None:
    queue = EventQueue()
    event = queue.schedule(10, kind="x", action=noop)
    assert queue.cancel(event) is True
    assert queue.cancel(event) is False


def test_cancelling_an_already_fired_event_is_false() -> None:
    queue = EventQueue()
    event = queue.schedule(10, kind="x", action=noop)
    assert queue.pop() is event
    assert queue.cancel(event) is False


def test_length_counts_live_events_only() -> None:
    queue = EventQueue()
    a = queue.schedule(10, kind="a", action=noop)
    queue.schedule(20, kind="b", action=noop)
    assert len(queue) == 2
    queue.cancel(a)
    assert len(queue) == 1
    queue.pop()
    assert len(queue) == 0


def test_peek_tick_skips_cancelled_events() -> None:
    queue = EventQueue()
    early = queue.schedule(10, kind="early", action=noop)
    queue.schedule(20, kind="late", action=noop)
    assert queue.peek_tick() == 10
    queue.cancel(early)
    assert queue.peek_tick() == 20


def test_peek_tick_is_none_when_empty() -> None:
    queue = EventQueue()
    assert queue.peek_tick() is None
    queue.cancel(queue.schedule(5, kind="x", action=noop))
    assert queue.peek_tick() is None


def test_pending_is_sorted_and_excludes_cancelled() -> None:
    queue = EventQueue()
    queue.schedule(30, kind="c", action=noop)
    gone = queue.schedule(10, kind="a", action=noop)
    queue.schedule(20, kind="b", action=noop)
    queue.cancel(gone)
    assert [e.kind for e in queue.pending()] == ["b", "c"]


def test_scheduling_in_the_past_is_rejected() -> None:
    queue = EventQueue()
    with pytest.raises(ValueError, match="negative tick"):
        queue.schedule(-1, kind="x", action=noop)


def test_sequence_numbers_never_repeat() -> None:
    queue = EventQueue()
    seqs = [queue.schedule(0, kind="x", action=noop).seq for _ in range(100)]
    assert seqs == list(range(100))
    queue.pop()
    assert queue.schedule(0, kind="x", action=noop).seq == 100


def test_describe_is_json_shaped() -> None:
    queue = EventQueue()
    event = queue.schedule(7, kind="deliver", action=noop, actor=3, detail={"msg": "ping"})
    assert event.describe() == {
        "tick": 7,
        "seq": 0,
        "kind": "deliver",
        "actor": 3,
        "detail": {"msg": "ping"},
    }


def test_describe_omits_empty_fields() -> None:
    queue = EventQueue()
    event = queue.schedule(7, kind="timer", action=noop)
    assert event.describe() == {"tick": 7, "seq": 0, "kind": "timer"}


def test_actions_run_when_popped() -> None:
    queue = EventQueue()
    fired: list[str] = []
    queue.schedule(2, kind="b", action=lambda: fired.append("b"))
    queue.schedule(1, kind="a", action=lambda: fired.append("a"))
    for event in drain(queue):
        event.action()
    assert fired == ["a", "b"]
