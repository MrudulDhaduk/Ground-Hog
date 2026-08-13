"""Virtual time: it only moves when the queue says so."""

import pytest

from groundhog.clock import SimClock
from groundhog.sim.event import EventQueue
from groundhog.types import MILLISECOND


def build() -> tuple[EventQueue, SimClock]:
    queue = EventQueue()
    return queue, SimClock(queue)


def drain(queue: EventQueue, clock: SimClock) -> None:
    """A stand-in for the simulator loop."""
    while (event := queue.pop()) is not None:
        clock.advance_to(event.tick)
        event.action()


def test_time_starts_at_zero() -> None:
    _, clock = build()
    assert clock.now() == 0


def test_time_does_not_pass_on_its_own() -> None:
    queue, clock = build()
    clock.after(10 * MILLISECOND, lambda: None)
    assert clock.now() == 0
    assert queue.peek_tick() == 10 * MILLISECOND
    assert clock.now() == 0


def test_after_fires_at_now_plus_delay() -> None:
    queue, clock = build()
    fired: list[int] = []
    clock.after(30, lambda: fired.append(clock.now()))
    drain(queue, clock)
    assert fired == [30]


def test_delays_compose_from_the_advanced_now() -> None:
    queue, clock = build()
    fired: list[int] = []

    def first() -> None:
        fired.append(clock.now())
        clock.after(5, second)

    def second() -> None:
        fired.append(clock.now())

    clock.after(10, first)
    drain(queue, clock)
    assert fired == [10, 15]


def test_zero_delay_fires_at_the_current_tick() -> None:
    queue, clock = build()
    fired: list[int] = []
    clock.after(0, lambda: fired.append(clock.now()))
    drain(queue, clock)
    assert fired == [0]


def test_negative_delay_is_rejected() -> None:
    _, clock = build()
    with pytest.raises(ValueError, match="into the past"):
        clock.after(-1, lambda: None)


def test_cancelled_timers_never_fire() -> None:
    queue, clock = build()
    fired: list[str] = []
    timer = clock.after(10, lambda: fired.append("boom"))
    assert timer.cancel() is True
    drain(queue, clock)
    assert fired == []


def test_cancel_reports_false_once_it_has_fired() -> None:
    queue, clock = build()
    timer = clock.after(10, lambda: None)
    drain(queue, clock)
    assert timer.cancel() is False


def test_timer_exposes_its_deadline() -> None:
    _, clock = build()
    timer = clock.after(25, lambda: None)
    assert timer.deadline == 25


def test_resetting_a_timer_pushes_the_deadline_out() -> None:
    """The election-timeout pattern: cancel the old one, arm a new one."""
    queue, clock = build()
    fired: list[int] = []

    first = clock.after(100, lambda: fired.append(clock.now()))

    def reset() -> None:
        first.cancel()
        clock.after(100, lambda: fired.append(clock.now()))

    clock.after(50, reset)
    drain(queue, clock)
    assert fired == [150]


def test_time_cannot_run_backwards() -> None:
    _, clock = build()
    clock.advance_to(100)
    with pytest.raises(RuntimeError, match="backwards"):
        clock.advance_to(99)


def test_advancing_to_the_same_tick_is_allowed() -> None:
    _, clock = build()
    clock.advance_to(100)
    clock.advance_to(100)
    assert clock.now() == 100
