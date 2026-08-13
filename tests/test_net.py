"""Deliver, delay, drop, duplicate, reorder -- and the ways a node stops listening."""

from itertools import pairwise
from typing import Any

import pytest
from conftest import ListTrace

from groundhog.clock import SimClock
from groundhog.sim.event import EventQueue
from groundhog.sim.net import NetworkFaults, NodeState, Partition, SimNetwork
from groundhog.sim.rng import Rng
from groundhog.types import MILLISECOND, NodeId

NO_LATENCY = (0, 0)


class Harness:
    """A network with three nodes and a hand-cranked event loop."""

    def __init__(self, seed: int = 1, node_ids: tuple[NodeId, ...] = (1, 2, 3), **faults: Any):
        self.rng = Rng(seed)
        self.queue = EventQueue()
        self.clock = SimClock(self.queue)
        self.trace = ListTrace()
        self.net: SimNetwork[str] = SimNetwork(
            self.rng, self.clock, self.trace, faults=NetworkFaults(**faults)
        )
        self.received: list[tuple[NodeId, NodeId, str]] = []
        self.links = {
            node_id: self.net.register(node_id, self._recorder(node_id)) for node_id in node_ids
        }

    def _recorder(self, node_id: NodeId):  # type: ignore[no-untyped-def]
        def on_message(frm: NodeId, msg: str) -> None:
            self.received.append((frm, node_id, msg))

        return on_message

    def drain(self) -> None:
        while (event := self.queue.pop()) is not None:
            self.clock.advance_to(event.tick)
            event.action()

    def messages(self) -> list[str]:
        return [msg for _, _, msg in self.received]


# -- the happy path -------------------------------------------------------------------


def test_a_message_arrives() -> None:
    world = Harness()
    world.links[1].send(2, "hello")
    world.drain()
    assert world.received == [(1, 2, "hello")]


def test_the_sender_cannot_be_forged() -> None:
    """A link is bound to one node, so `frm` is not a parameter anyone can set."""
    world = Harness()
    world.links[3].send(1, "hi")
    world.drain()
    assert world.received == [(3, 1, "hi")]


def test_delivery_happens_later_not_now() -> None:
    world = Harness(latency=(5 * MILLISECOND, 5 * MILLISECOND))
    world.links[1].send(2, "hello")
    assert world.received == []
    assert world.queue.peek_tick() == 5 * MILLISECOND
    world.drain()
    assert world.clock.now() == 5 * MILLISECOND


def test_latency_stays_inside_the_configured_range() -> None:
    low, high = 3 * MILLISECOND, 9 * MILLISECOND
    world = Harness(latency=(low, high))
    arrivals: list[int] = []
    for _ in range(100):
        world.links[1].send(2, "x")
        world.drain()
        arrivals.append(world.clock.now())
    gaps = [b - a for a, b in pairwise(arrivals)]
    assert all(low <= gap <= high for gap in gaps), gaps


def test_sending_to_a_stranger_is_a_bug_not_a_drop() -> None:
    world = Harness()
    with pytest.raises(ValueError, match="no such node"):
        world.links[1].send(99, "hello")


# -- loss and duplication -------------------------------------------------------------


def test_everything_can_be_dropped() -> None:
    world = Harness(drop_percent=100)
    world.links[1].send(2, "gone")
    world.drain()
    assert world.received == []
    assert world.net.dropped == 1
    assert world.trace.of_kind("net.drop")


def test_duplicates_arrive_twice() -> None:
    world = Harness(duplicate_percent=100)
    world.links[1].send(2, "echo")
    world.drain()
    assert world.messages() == ["echo", "echo"]
    assert world.trace.of_kind("net.duplicate")


def test_a_disabled_drop_still_consumes_a_draw() -> None:
    """Turning a fault off must not shift the rest of the seed's stream."""
    world = Harness(drop_percent=0, duplicate_percent=0, latency=NO_LATENCY)
    world.links[1].send(2, "x")
    assert world.rng.calls == 3  # drop check, latency, duplicate check


def test_reordering_happens_on_its_own() -> None:
    """Never implemented, only permitted: two independent latency draws are enough.

    If this ever fails, the network has quietly become a FIFO and a whole class of
    Raft bug -- an old AppendEntries overtaking a new one -- stops being reachable.
    """
    reordered = 0
    for seed in range(50):
        world = Harness(seed, latency=(1 * MILLISECOND, 20 * MILLISECOND))
        world.links[1].send(2, "first")
        world.links[1].send(2, "second")
        world.drain()
        if world.messages() == ["second", "first"]:
            reordered += 1
    assert reordered > 0, "50 seeds and not one overtake"


def test_a_duplicate_can_overtake_the_original() -> None:
    seen = 0
    for seed in range(50):
        world = Harness(seed, duplicate_percent=100, latency=(1 * MILLISECOND, 20 * MILLISECOND))
        world.links[1].send(2, "x")
        world.drain()
        kinds = [record["kind"] for record in world.trace.records]
        if "net.duplicate" in kinds:
            seen += 1
    assert seen == 50


# -- partitions -----------------------------------------------------------------------


def test_a_partition_blocks_both_directions() -> None:
    world = Harness()
    world.net.split(Partition(side_a=(1,), side_b=(2, 3)))
    world.links[1].send(2, "a->b")
    world.links[2].send(1, "b->a")
    world.drain()
    assert world.received == []
    assert world.net.dropped == 2


def test_a_one_way_partition_blocks_one_direction_only() -> None:
    """The evil one: node 1 hears everyone, and nobody hears node 1."""
    world = Harness()
    world.net.split(Partition(side_a=(1,), side_b=(2, 3), one_way=True))
    world.links[1].send(2, "lost")
    world.links[2].send(1, "delivered")
    world.drain()
    assert world.messages() == ["delivered"]


def test_nodes_on_the_same_side_still_talk() -> None:
    world = Harness()
    world.net.split(Partition(side_a=(1,), side_b=(2, 3)))
    world.links[2].send(3, "fine")
    world.drain()
    assert world.messages() == ["fine"]


def test_a_partition_kills_messages_already_in_flight() -> None:
    """Checked at delivery, not at send. A packet crossing the break when it opens is
    a packet that dies -- and those are the ones that produce interesting bugs."""
    world = Harness(latency=(10 * MILLISECOND, 10 * MILLISECOND))
    world.links[1].send(2, "in flight")
    world.net.split(Partition(side_a=(1,), side_b=(2, 3)))
    world.drain()
    assert world.received == []


def test_healing_lets_traffic_through_again() -> None:
    world = Harness()
    world.net.split(Partition(side_a=(1,), side_b=(2, 3)))
    world.net.heal()
    world.links[1].send(2, "welcome back")
    world.drain()
    assert world.messages() == ["welcome back"]
    assert world.trace.of_kind("net.heal")


@pytest.mark.parametrize(
    ("frm", "to", "blocked"),
    [(1, 2, True), (2, 1, True), (1, 3, False), (2, 3, True), (3, 3, False)],
)
def test_partition_membership(frm: NodeId, to: NodeId, blocked: bool) -> None:
    assert Partition(side_a=(1, 3), side_b=(2,)).blocks(frm, to) is blocked


# -- crashed and hung nodes -----------------------------------------------------------


def test_messages_to_a_crashed_node_are_lost() -> None:
    world = Harness()
    world.net.set_state(2, NodeState.CRASHED)
    world.links[1].send(2, "into the void")
    world.drain()
    assert world.received == []
    assert world.net.dropped == 1


def test_a_hung_node_holds_its_messages_and_gets_them_all_at_once() -> None:
    """The nastier failure: everyone else still thinks it is fine."""
    world = Harness()
    world.net.set_state(2, NodeState.HUNG)
    for label in ("one", "two", "three"):
        world.links[1].send(2, label)
    world.drain()
    assert world.received == []
    assert world.trace.of_kind("net.hold")

    world.net.set_state(2, NodeState.RUNNING)
    world.drain()
    assert world.trace.of_kind("net.release")

    # Waking up drains a buffer, so the order is the order things *arrived* in -- which
    # the network may already have shuffled on the way. Send order is not the invariant
    # here; arrival order is.
    arrived = [str(record["msg"]) for record in world.trace.of_kind("net.hold")]
    assert world.messages() == arrived
    assert sorted(arrived) == ["one", "three", "two"]


def test_a_hung_node_that_crashes_loses_what_it_was_holding() -> None:
    world = Harness()
    world.net.set_state(2, NodeState.HUNG)
    world.links[1].send(2, "held")
    world.drain()

    world.net.set_state(2, NodeState.CRASHED)
    world.net.set_state(2, NodeState.RUNNING)
    world.drain()
    assert world.received == []


def test_a_node_starts_out_running() -> None:
    world = Harness()
    assert world.net.state(1) is NodeState.RUNNING


def test_a_node_cannot_join_twice() -> None:
    world = Harness()
    with pytest.raises(ValueError, match="already on the network"):
        world.net.register(1, lambda frm, msg: None)


def test_the_switchboard_lists_its_nodes_in_order() -> None:
    world = Harness(node_ids=(3, 1, 2))
    assert world.net.node_ids() == [1, 2, 3]
