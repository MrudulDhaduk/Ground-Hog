"""Two toy nodes throwing a ball at each other. The M1 proof of life.

There is no network yet (that is M3) and no consensus (M5). This exists to put every
piece of the deterministic core under load at once -- rng, event queue, virtual clock,
timer cancellation, trace -- so that "the same seed gives the same trace" is a claim
about something non-trivial rather than about an empty file.

The shape is deliberately Raft-flavoured: a periodic timer that fires unless a message
arrives first and resets it. That is an election timeout with the consensus removed.
"""

from dataclasses import dataclass
from typing import Final

from groundhog.clock import Timer
from groundhog.sim.trace import Trace
from groundhog.sim.world import RunResult, Simulator
from groundhog.types import MILLISECOND, SECOND, NodeId, Tick

PING_PERIOD: Final = (20 * MILLISECOND, 40 * MILLISECOND)
LATENCY: Final = (1 * MILLISECOND, 15 * MILLISECOND)
REPLY_DELAY: Final = (1 * MILLISECOND, 10 * MILLISECOND)
DROP_PERCENT: Final = 5

DEFAULT_MAX_TICKS: Final[Tick] = 1 * SECOND


@dataclass(slots=True)
class PingPongNode:
    node_id: NodeId
    peer_id: NodeId
    sent: int = 0
    received: int = 0
    dropped: int = 0
    timer: Timer | None = None


class PingPongWorld:
    """Wires two nodes into a simulator and keeps them talking."""

    def __init__(self, sim: Simulator[PingPongNode]) -> None:
        self.sim = sim
        for node_id, peer_id in ((1, 2), (2, 1)):
            sim.register(node_id, PingPongNode(node_id=node_id, peer_id=peer_id))

    def start(self) -> None:
        for node_id in self.sim.node_ids():
            self._arm_ping(node_id)

    def _arm_ping(self, node_id: NodeId) -> None:
        """(Re)start a node's ping timer. The cancellation path is the point."""
        node = self.sim.nodes[node_id]
        if node.timer is not None:
            node.timer.cancel()
        delay = self.sim.rng.between(*PING_PERIOD)
        node.timer = self.sim.clock.after(
            delay,
            lambda: self._on_ping_timer(node_id),
            name="ping_timer",
            actor=node_id,
        )

    def _on_ping_timer(self, node_id: NodeId) -> None:
        node = self.sim.nodes[node_id]
        self._send(node_id, node.peer_id, "ping")
        self._arm_ping(node_id)

    def _send(self, frm: NodeId, to: NodeId, label: str) -> None:
        sender = self.sim.nodes[frm]
        sender.sent += 1
        if self.sim.rng.chance(DROP_PERCENT):
            sender.dropped += 1
            self.sim.trace.write(
                {"kind": "drop", "tick": self.sim.clock.now(), "frm": frm, "to": to, "msg": label}
            )
            return
        self.sim.trace.write(
            {"kind": "send", "tick": self.sim.clock.now(), "frm": frm, "to": to, "msg": label}
        )
        self.sim.clock.after(
            self.sim.rng.between(*LATENCY),
            lambda: self._deliver(frm, to, label),
            name=f"deliver:{label}",
            actor=to,
        )

    def _deliver(self, frm: NodeId, to: NodeId, label: str) -> None:
        self.sim.nodes[to].received += 1
        if label == "ping":
            self.sim.clock.after(
                self.sim.rng.between(*REPLY_DELAY),
                lambda: self._send(to, frm, "pong"),
                name="reply",
                actor=to,
            )
        else:
            # A pong is proof the peer is alive, so the ping timer starts over.
            self._arm_ping(to)


def build_demo(seed: int, *, trace: Trace) -> Simulator[PingPongNode]:
    sim: Simulator[PingPongNode] = Simulator(seed, trace=trace, header={"demo": "pingpong"})
    PingPongWorld(sim).start()
    return sim


def run_demo(
    seed: int,
    *,
    trace: Trace,
    max_ticks: Tick = DEFAULT_MAX_TICKS,
) -> tuple[Simulator[PingPongNode], RunResult]:
    sim = build_demo(seed, trace=trace)
    return sim, sim.run(max_ticks)
