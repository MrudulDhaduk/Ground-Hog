"""The simulator: one loop, one thread, one seed.

The whole program collapses to a pure function of the seed. Everything that would
normally come from outside -- the clock, the scheduler, the network, the disk -- is
either owned by this object or reached through a handle it hands out.

The loop itself is deliberately boring:

    take the earliest event -> move time to it -> record it -> run it

Nothing else. Anything that wants to happen later does so by scheduling an event, which
means the entire history of a run is a single ordered list, and that list is the trace.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Generic, TypeVar

from groundhog.clock import SimClock
from groundhog.sim.event import EventQueue
from groundhog.sim.rng import Rng
from groundhog.sim.trace import NullTrace, Trace
from groundhog.types import JsonValue, NodeId, Tick

NodeT = TypeVar("NodeT")

#: Why `run()` returned.
STOP_IDLE = "idle"
STOP_MAX_TICKS = "max_ticks"


@dataclass(frozen=True, slots=True)
class RunResult:
    seed: int
    events: int
    final_tick: Tick
    rng_calls: int
    stop_reason: str


class Simulator(Generic[NodeT]):
    """Owns the seed, the clock, the queue and the nodes.

    Generic over the node type so `sim.nodes[1]` is typed: M1 runs `Simulator[PingPong]`,
    M5 runs `Simulator[RaftNode]`, and neither needs a cast.
    """

    def __init__(
        self,
        seed: int,
        *,
        trace: Trace | None = None,
        header: Mapping[str, JsonValue] | None = None,
    ) -> None:
        self.rng = Rng(seed)
        self.queue = EventQueue()
        self.clock = SimClock(self.queue)
        self.trace: Trace = trace if trace is not None else NullTrace()
        self.nodes: dict[NodeId, NodeT] = {}
        #: Extra fields for the `sim.start` record. Mutable until `run()` is called,
        #: because some of what belongs in a header -- M3's fault schedule -- has to be
        #: drawn from `self.rng`, which does not exist until this object does.
        self.header: dict[str, JsonValue] = dict(header) if header is not None else {}
        self._events = 0

    def register(self, node_id: NodeId, node: NodeT) -> None:
        if node_id in self.nodes:
            raise ValueError(f"node {node_id} is already registered")
        self.nodes[node_id] = node

    def node_ids(self) -> list[NodeId]:
        """Sorted, always. Never iterate `self.nodes` for an ordering decision."""
        return sorted(self.nodes)

    def run(self, max_ticks: Tick) -> RunResult:
        """Drain the queue until it is empty or the next event is past `max_ticks`."""
        start: dict[str, JsonValue] = {
            "kind": "sim.start",
            "seed": self.rng.seed,
            "max_ticks": max_ticks,
            "nodes": self.node_ids(),
        }
        start.update(self.header)
        self.trace.write(start)

        stop_reason = STOP_IDLE
        while True:
            next_tick = self.queue.peek_tick()
            if next_tick is None:
                break
            if next_tick > max_ticks:
                stop_reason = STOP_MAX_TICKS
                break

            event = self.queue.pop()
            if event is None:
                break

            self.clock.advance_to(event.tick)
            self._events += 1

            # Recorded *before* it runs, so an event that raises still appears in the
            # trace. The last line of a crashed run is the thing that crashed it.
            record = event.describe()
            record["rng"] = self.rng.calls
            self.trace.write(record)

            try:
                event.action()
            except Exception as exc:
                self.trace.write(
                    {
                        "kind": "sim.error",
                        "tick": self.clock.now(),
                        "seq": event.seq,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                raise

        result = RunResult(
            seed=self.rng.seed,
            events=self._events,
            final_tick=self.clock.now(),
            rng_calls=self.rng.calls,
            stop_reason=stop_reason,
        )
        self.trace.write(
            {
                "kind": "sim.end",
                "tick": result.final_tick,
                "events": result.events,
                "rng": result.rng_calls,
                "stop_reason": result.stop_reason,
            }
        )
        return result
