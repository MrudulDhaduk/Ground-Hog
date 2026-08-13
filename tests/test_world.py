"""The run loop itself: what it records, when it stops, how it fails."""

from collections.abc import Mapping

import pytest

from groundhog.sim.trace import NullTrace
from groundhog.sim.world import STOP_IDLE, STOP_MAX_TICKS, Simulator
from groundhog.types import JsonValue


class ListTrace:
    def __init__(self) -> None:
        self.records: list[Mapping[str, JsonValue]] = []

    def write(self, record: Mapping[str, JsonValue]) -> None:
        self.records.append(record)

    def kinds(self) -> list[str]:
        return [str(record["kind"]) for record in self.records]


def test_an_empty_world_stops_idle() -> None:
    sim: Simulator[str] = Simulator(1)
    result = sim.run(1000)
    assert result.events == 0
    assert result.stop_reason == STOP_IDLE
    assert result.final_tick == 0


def test_max_ticks_stops_the_run_before_the_next_event() -> None:
    sim: Simulator[str] = Simulator(1)
    fired: list[str] = []
    sim.clock.after(100, lambda: fired.append("early"))
    sim.clock.after(5000, lambda: fired.append("late"))
    result = sim.run(1000)
    assert fired == ["early"]
    assert result.stop_reason == STOP_MAX_TICKS
    assert result.final_tick == 100
    assert len(sim.queue) == 1


def test_an_event_exactly_on_the_limit_still_runs() -> None:
    sim: Simulator[str] = Simulator(1)
    fired: list[str] = []
    sim.clock.after(1000, lambda: fired.append("edge"))
    assert sim.run(1000).stop_reason == STOP_IDLE
    assert fired == ["edge"]


def test_events_scheduled_during_the_run_are_picked_up() -> None:
    sim: Simulator[str] = Simulator(1)
    fired: list[int] = []

    def step(remaining: int) -> None:
        fired.append(sim.clock.now())
        if remaining:
            sim.clock.after(10, lambda: step(remaining - 1))

    sim.clock.after(10, lambda: step(3))
    result = sim.run(10_000)
    assert fired == [10, 20, 30, 40]
    assert result.events == 4


def test_registering_the_same_node_twice_is_a_bug() -> None:
    sim: Simulator[str] = Simulator(1)
    sim.register(1, "a")
    with pytest.raises(ValueError, match="already registered"):
        sim.register(1, "b")


def test_node_ids_are_sorted_not_insertion_ordered() -> None:
    sim: Simulator[str] = Simulator(1)
    for node_id in (3, 1, 2):
        sim.register(node_id, f"node{node_id}")
    assert sim.node_ids() == [1, 2, 3]


def test_the_trace_is_bracketed_by_start_and_end() -> None:
    trace = ListTrace()
    sim: Simulator[str] = Simulator(7, trace=trace)
    sim.register(2, "b")
    sim.register(1, "a")
    sim.clock.after(5, lambda: None, name="tick", actor=1)
    sim.run(100)

    assert trace.kinds() == ["sim.start", "tick", "sim.end"]
    start = trace.records[0]
    assert start["seed"] == 7
    assert start["nodes"] == [1, 2]
    end = trace.records[-1]
    assert end["events"] == 1
    assert end["stop_reason"] == STOP_IDLE


def test_the_header_is_carried_into_the_trace() -> None:
    """M3 puts the whole fault schedule here."""
    trace = ListTrace()
    sim: Simulator[str] = Simulator(7, trace=trace, header={"faults": "aggressive"})
    sim.run(10)
    assert trace.records[0]["faults"] == "aggressive"


def test_each_event_line_carries_the_rng_counter() -> None:
    trace = ListTrace()
    sim: Simulator[str] = Simulator(7, trace=trace)

    def draw() -> None:
        sim.rng.below(100)

    sim.clock.after(5, draw, name="draws")
    sim.clock.after(10, lambda: None, name="quiet")
    sim.run(100)

    draws = next(record for record in trace.records if record["kind"] == "draws")
    quiet = next(record for record in trace.records if record["kind"] == "quiet")
    assert draws["rng"] == 0  # recorded before the event ran
    assert quiet["rng"] == 1


def test_a_failing_event_is_recorded_before_it_is_reraised() -> None:
    trace = ListTrace()
    sim: Simulator[str] = Simulator(7, trace=trace)

    def explode() -> None:
        raise ZeroDivisionError("boom")

    sim.clock.after(5, explode, name="explode")
    with pytest.raises(ZeroDivisionError, match="boom"):
        sim.run(100)

    assert trace.kinds() == ["sim.start", "explode", "sim.error"]
    assert trace.records[-1]["error"] == "ZeroDivisionError: boom"
    assert trace.records[-1]["tick"] == 5


def test_the_default_trace_discards_everything() -> None:
    sim: Simulator[str] = Simulator(1)
    assert isinstance(sim.trace, NullTrace)
    sim.clock.after(5, lambda: None)
    assert sim.run(100).events == 1


def test_cancelled_events_do_not_count_as_events() -> None:
    trace = ListTrace()
    sim: Simulator[str] = Simulator(1, trace=trace)
    timer = sim.clock.after(5, lambda: None, name="doomed")
    sim.clock.after(10, lambda: None, name="survivor")
    timer.cancel()
    result = sim.run(100)
    assert result.events == 1
    assert trace.kinds() == ["sim.start", "survivor", "sim.end"]
