"""A network that drops, delays, duplicates, reorders and splits in half.

Everything a network can do to you fits on one line: **deliver, delay, drop, duplicate,
reorder**. That is the whole menu. Production does not invent a sixth, which is why
covering these five is worth something rather than being a party trick.

Reordering is not implemented. It is *emergent*: two messages sent back to back get
independent latency draws, so the second overtakes the first whenever its draw is
smaller. Anything that produced reordering by explicitly shuffling a queue would be
modelling the symptom instead of the cause. `test_net.py` asserts it actually happens,
because an emergent property nobody checked is just a hope.

When faults are evaluated
-------------------------
- **drop** at send time -- the packet dies somewhere, and the sender never learns where.
- **partition, crash, hang** at *delivery* time. A message already in flight when the
  network splits is a message that was travelling through the break when it happened,
  so it dies too. Checking at send time would quietly deliver every message that was
  lucky enough to leave first, and those in-flight messages are exactly the ones that
  make interesting Raft bugs.
"""

import enum
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Generic, TypeVar

from groundhog.clock import SimClock
from groundhog.sim.rng import Rng
from groundhog.sim.trace import Trace
from groundhog.types import MILLISECOND, JsonValue, NodeId, Tick

M = TypeVar("M")


class NodeState(enum.Enum):
    """What a node is doing, from the network's point of view."""

    RUNNING = "running"
    #: Dead. Messages aimed at it are dropped; it will come back with an empty memory.
    CRASHED = "crashed"
    #: Alive but not answering -- a stop-the-world pause, a stalled disk, a wedged
    #: thread. Messages are *held*, not lost, and arrive in a burst when it wakes up.
    #: This is nastier than a crash: everyone else still thinks it is fine.
    HUNG = "hung"


@dataclass(frozen=True, slots=True)
class NetworkFaults:
    #: Inclusive range for one-way delivery latency.
    latency: tuple[Tick, Tick] = (1 * MILLISECOND, 10 * MILLISECOND)
    drop_percent: int = 0
    duplicate_percent: int = 0

    def describe(self) -> Mapping[str, JsonValue]:
        return {
            "latency": list(self.latency),
            "drop_percent": self.drop_percent,
            "duplicate_percent": self.duplicate_percent,
        }


@dataclass(frozen=True, slots=True)
class Partition:
    """A split of the cluster into two groups that cannot talk.

    `one_way` models the genuinely evil version: A hears B perfectly well, B hears
    nothing from A. Real networks do this (asymmetric routing, a one-directional
    firewall rule) and it breaks naive failure detectors, because the silent side sees
    a healthy peer while the other side sees a dead one.
    """

    side_a: tuple[NodeId, ...]
    side_b: tuple[NodeId, ...]
    one_way: bool = False

    def blocks(self, frm: NodeId, to: NodeId) -> bool:
        if frm in self.side_a and to in self.side_b:
            return True
        return not self.one_way and frm in self.side_b and to in self.side_a

    def describe(self) -> Mapping[str, JsonValue]:
        return {
            "side_a": list(self.side_a),
            "side_b": list(self.side_b),
            "one_way": self.one_way,
        }


@dataclass(frozen=True, slots=True)
class NodeLink(Generic[M]):
    """The handle a single node holds. It cannot address anyone but its peers, and it
    cannot claim to be anyone but itself."""

    switch: "SimNetwork[M]"
    node_id: NodeId

    def send(self, to: NodeId, msg: M) -> None:
        self.switch.send(self.node_id, to, msg)


class SimNetwork(Generic[M]):
    """The switchboard. Owns latency, loss and who can currently hear whom."""

    def __init__(
        self,
        rng: Rng,
        clock: SimClock,
        trace: Trace,
        *,
        faults: NetworkFaults | None = None,
        describe: Callable[[M], JsonValue] = str,
    ) -> None:
        self.rng = rng
        self.clock = clock
        self.trace = trace
        self.faults = faults if faults is not None else NetworkFaults()
        self._describe = describe
        self._inboxes: dict[NodeId, Callable[[NodeId, M], None]] = {}
        self._state: dict[NodeId, NodeState] = {}
        self._held: dict[NodeId, list[tuple[NodeId, M]]] = {}
        self.partition: Partition | None = None
        self.delivered = 0
        self.dropped = 0

    # -- wiring --------------------------------------------------------------

    def register(self, node_id: NodeId, on_message: Callable[[NodeId, M], None]) -> NodeLink[M]:
        if node_id in self._inboxes:
            raise ValueError(f"node {node_id} is already on the network")
        self._inboxes[node_id] = on_message
        self._state[node_id] = NodeState.RUNNING
        self._held[node_id] = []
        return NodeLink(switch=self, node_id=node_id)

    def node_ids(self) -> list[NodeId]:
        return sorted(self._inboxes)

    def state(self, node_id: NodeId) -> NodeState:
        return self._state[node_id]

    def set_state(self, node_id: NodeId, state: NodeState) -> None:
        previous = self._state[node_id]
        self._state[node_id] = state
        if state is NodeState.CRASHED:
            # A crashed node's held messages die with it. This branch has to come
            # first: hung -> crashed must discard, not deliver.
            self._held[node_id].clear()
        elif previous is NodeState.HUNG:
            self._release(node_id)

    def split(self, partition: Partition) -> None:
        self.partition = partition
        self._record("net.partition", partition.describe())

    def heal(self) -> None:
        if self.partition is not None:
            self._record("net.heal", self.partition.describe())
        self.partition = None

    # -- the network itself --------------------------------------------------

    def send(self, frm: NodeId, to: NodeId, msg: M) -> None:
        if to not in self._inboxes:
            raise ValueError(f"no such node: {to}")

        # Drawn whether or not loss is enabled, so switching the fault off does not
        # shift the rest of the seed's stream.
        if self.rng.chance(self.faults.drop_percent):
            self.dropped += 1
            self._record("net.drop", {"frm": frm, "to": to, "msg": self._describe(msg)})
            return

        self._schedule(frm, to, msg, tag="net.deliver")
        if self.rng.chance(self.faults.duplicate_percent):
            # An independent latency draw, which is why the copy can arrive first.
            self._schedule(frm, to, msg, tag="net.deliver_dup")
            self._record("net.duplicate", {"frm": frm, "to": to, "msg": self._describe(msg)})

    def _schedule(
        self, frm: NodeId, to: NodeId, msg: M, *, tag: str, delay: Tick | None = None
    ) -> None:
        self.clock.after(
            self.rng.between(*self.faults.latency) if delay is None else delay,
            lambda: self._arrive(frm, to, msg),
            name=tag,
            actor=to,
        )

    def _arrive(self, frm: NodeId, to: NodeId, msg: M) -> None:
        if self.partition is not None and self.partition.blocks(frm, to):
            self.dropped += 1
            self._record("net.drop", {"frm": frm, "to": to, "msg": self._describe(msg)})
            return

        state = self._state[to]
        if state is NodeState.CRASHED:
            self.dropped += 1
            self._record("net.drop", {"frm": frm, "to": to, "msg": self._describe(msg)})
            return
        if state is NodeState.HUNG:
            self._held[to].append((frm, msg))
            self._record("net.hold", {"frm": frm, "to": to, "msg": self._describe(msg)})
            return

        self.delivered += 1
        self._inboxes[to](frm, msg)

    def _release(self, node_id: NodeId) -> None:
        """A hung node wakes up and everything it missed lands at once.

        Zero delay, in arrival order. A hang is the *application* not reading its
        socket; the bytes were already on the machine, so waking up drains a buffer
        rather than re-crossing the network. The queue breaks ties by insertion order,
        so "at the same tick, in order" is exactly what happens.
        """
        held = self._held[node_id]
        if not held:
            return
        self._held[node_id] = []
        self._record("net.release", {"to": node_id, "count": len(held)})
        for frm, msg in held:
            self._schedule(frm, node_id, msg, tag="net.deliver_held", delay=0)

    def _record(self, kind: str, detail: Mapping[str, JsonValue]) -> None:
        record: dict[str, JsonValue] = {"kind": kind, "tick": self.clock.now()}
        record.update(detail)
        self.trace.write(record)
