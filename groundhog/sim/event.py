"""The event queue. Virtual time is whatever this thing says it is.

Determinism rule 7: heap entries are `(tick, seq, event)`. `seq` is a monotonically
increasing integer, so no two entries are ever equal on the first two fields and the
heap **never compares an `Event`**. That matters more than it looks: if ties were broken
by payload, two events at the same tick would order by object contents or -- worse, for
objects without `__lt__` -- by nothing reproducible at all.

`Event` is built with `eq=False` and no ordering, so it has no `__lt__`. If a future
change ever lets the heap reach the third element, Python raises `TypeError` on the
spot instead of silently picking an order.
"""

import heapq
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TypeAlias

from groundhog.types import JsonValue, NodeId, Tick

#: What an event does when its time comes. Side effects only; no return value.
Action: TypeAlias = Callable[[], None]


@dataclass(frozen=True, slots=True, eq=False)
class Event:
    """One thing that happens at one tick."""

    tick: Tick
    seq: int
    kind: str
    action: Action
    actor: NodeId | None = None
    detail: Mapping[str, JsonValue] = field(default_factory=dict)

    def describe(self) -> dict[str, JsonValue]:
        """The trace form. Everything except the callable, which is not serialisable."""
        record: dict[str, JsonValue] = {
            "tick": self.tick,
            "seq": self.seq,
            "kind": self.kind,
        }
        if self.actor is not None:
            record["actor"] = self.actor
        if self.detail:
            record["detail"] = dict(self.detail)
        return record


class EventQueue:
    """A priority queue of pending events, ordered by `(tick, seq)`.

    Cancellation is lazy: a cancelled event stays in the heap and is skipped on the way
    out. Eager removal would mean an O(n) scan and a re-heapify on every election timer
    reset, which in Raft is most of what ever happens.
    """

    __slots__ = ("_heap", "_next_seq", "_pending")

    def __init__(self) -> None:
        self._heap: list[tuple[Tick, int, Event]] = []
        self._next_seq: int = 0
        # seq -> event, for everything scheduled and not yet popped or cancelled.
        # A dict, not a set: rule 3, and this one is iterated in `pending()`.
        self._pending: dict[int, Event] = {}

    def __len__(self) -> int:
        """Live events. Cancelled ones still sitting in the heap do not count."""
        return len(self._pending)

    def schedule(
        self,
        tick: Tick,
        *,
        kind: str,
        action: Action,
        actor: NodeId | None = None,
        detail: Mapping[str, JsonValue] | None = None,
    ) -> Event:
        if tick < 0:
            raise ValueError(f"cannot schedule at a negative tick: {tick}")
        event = Event(
            tick=tick,
            seq=self._next_seq,
            kind=kind,
            action=action,
            actor=actor,
            detail=detail if detail is not None else {},
        )
        self._next_seq += 1
        self._pending[event.seq] = event
        heapq.heappush(self._heap, (event.tick, event.seq, event))
        return event

    def cancel(self, event: Event) -> bool:
        """Drop a scheduled event. True if it was still pending."""
        return self._pending.pop(event.seq, None) is not None

    def pop(self) -> Event | None:
        """The earliest live event, or None when nothing is left."""
        while self._heap:
            _, seq, event = heapq.heappop(self._heap)
            if self._pending.pop(seq, None) is not None:
                return event
        return None

    def peek_tick(self) -> Tick | None:
        """When the next live event fires, without running it."""
        self._drop_cancelled_head()
        return self._heap[0][0] if self._heap else None

    def pending(self) -> list[Event]:
        """Live events in `(tick, seq)` order. For debugging and state dumps."""
        return sorted(self._pending.values(), key=lambda e: (e.tick, e.seq))

    def _drop_cancelled_head(self) -> None:
        while self._heap and self._heap[0][1] not in self._pending:
            heapq.heappop(self._heap)
