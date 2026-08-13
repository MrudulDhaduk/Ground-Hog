"""Time, behind an interface.

This is one of the three handles from spec §5 that must exist from day one: a Raft node
never asks the operating system what time it is, it asks its `Clock`. Swap the
implementation and the same node code runs in a simulation or on a real machine.

The Go sketch in the spec returns a channel from `After`. Python has no channels, and
introducing one would mean a thread, which rule 6 forbids. The equivalent that keeps
everything single-threaded is a callback plus a cancellable handle -- which is what a
Raft election timer needs anyway, since it gets reset on nearly every message.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from groundhog.sim.event import Event, EventQueue
from groundhog.types import NodeId, Tick


class Timer(Protocol):
    """A scheduled callback that has not fired yet."""

    def cancel(self) -> bool:
        """Stop it firing. True if it was still pending."""
        ...


class Clock(Protocol):
    def now(self) -> Tick: ...

    def after(
        self,
        delay: Tick,
        callback: Callable[[], None],
        *,
        name: str = "timer",
        actor: NodeId | None = None,
    ) -> Timer:
        """Run `callback` once, `delay` ticks from now.

        `name` and `actor` are labels for the trace. A real clock is free to ignore
        them; a simulated one puts them in the event log so a timeline can be
        reconstructed per node.
        """
        ...


@dataclass(frozen=True, slots=True)
class SimTimer:
    queue: EventQueue
    event: Event

    @property
    def deadline(self) -> Tick:
        return self.event.tick

    def cancel(self) -> bool:
        return self.queue.cancel(self.event)


class SimClock:
    """Virtual time. Advances only when the simulator pops an event.

    Nothing here polls or waits. Between two events, no time passes at all -- which is
    why a simulated run of an hour of cluster life takes milliseconds.
    """

    __slots__ = ("_now", "_queue")

    def __init__(self, queue: EventQueue) -> None:
        self._queue = queue
        self._now: Tick = 0

    def now(self) -> Tick:
        return self._now

    def after(
        self,
        delay: Tick,
        callback: Callable[[], None],
        *,
        name: str = "timer",
        actor: NodeId | None = None,
    ) -> SimTimer:
        if delay < 0:
            raise ValueError(f"cannot schedule into the past: delay={delay}")
        event = self._queue.schedule(self._now + delay, kind=name, action=callback, actor=actor)
        return SimTimer(queue=self._queue, event=event)

    def advance_to(self, tick: Tick) -> None:
        """Move time forward. Called by the simulator loop and by nobody else.

        Time going backwards means the queue handed out an event older than the one
        before it, which would mean the ordering invariant is broken. Fail loudly.
        """
        if tick < self._now:
            raise RuntimeError(f"time moved backwards: now={self._now}, requested={tick}")
        self._now = tick
