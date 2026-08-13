"""A toy cluster, so the harness has something to torture before Raft exists.

Three nodes ping each other on a timer and reply. There is no consensus here and no
agreement to violate -- the point is to put every part of the simulator under load at
once: rng, event queue, virtual clock, timer cancellation, the network with its whole
fault menu, the disk with its own, and the trace that has to come out identical twice.

The node deliberately has the shape M5's Raft node will have:

    a Clock, a Network handle, a Storage, some volatile state, some persistent state,
    and a `crash()` that keeps only what was synced.

`pongs` is the volatile state and the WAL is the persistent copy. Crash a node and the
counter is gone; restart it and the counter comes back from the log -- minus whatever
had not been synced, which is exactly the loss the M2 disk model promises.
"""

from collections.abc import Callable
from functools import partial
from typing import Final

from groundhog.clock import Clock, Timer
from groundhog.network import Network
from groundhog.sim.disk import SimStorage
from groundhog.sim.faults import PROFILES, FaultInjector, FaultProfile, generate_schedule
from groundhog.sim.net import NodeState, SimNetwork
from groundhog.sim.rng import Rng
from groundhog.sim.trace import Trace
from groundhog.sim.world import RunResult, Simulator
from groundhog.storage import DiskError
from groundhog.types import MILLISECOND, SECOND, JsonValue, NodeId, Tick

PING_PERIOD: Final = (20 * MILLISECOND, 40 * MILLISECOND)
SYNC_EVERY: Final = 5
#: How long a supervisor takes to notice a node died on its own and restart it.
RESTART_DELAY: Final[Tick] = 100 * MILLISECOND

DEFAULT_NODES: Final = 3
DEFAULT_MAX_TICKS: Final[Tick] = 1 * SECOND

PING: Final = "ping"
PONG: Final = "pong"


class DemoNode:
    """Sends pings, answers pings, remembers how many answers it got."""

    def __init__(
        self,
        node_id: NodeId,
        peers: tuple[NodeId, ...],
        clock: Clock,
        net: Network[str],
        storage: SimStorage,
        rng: Rng,
        trace: Trace,
        on_failure: Callable[[NodeId], None],
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.clock = clock
        self.net = net
        self.storage = storage
        self.rng = rng
        self.trace = trace
        #: Called when the node kills itself rather than being killed by the schedule.
        #: A process that dies of its own accord still needs somebody to notice.
        self.on_failure = on_failure

        # Volatile. A crash takes all of it.
        self.running = True
        self.pongs = 0
        self.unsynced = 0
        self.timer: Timer | None = None

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        self._arm()

    def crash(self) -> None:
        if not self.running:
            return
        self.running = False
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None
        self.pongs = 0
        self.unsynced = 0
        self.storage.crash()
        self._record("node.crash")

    def restart(self) -> None:
        self.storage.restart()
        self.pongs = len(self.storage.read_all())
        self.running = True
        self._record("node.restart", recovered=self.pongs)
        self._arm()

    # -- behaviour -----------------------------------------------------------

    def on_message(self, frm: NodeId, msg: str) -> None:
        if not self.running:
            return
        if msg == PING:
            self.net.send(frm, PONG)
            return

        self.pongs += 1
        try:
            self.storage.append([f"pong from {frm}".encode()])
        except DiskError:
            # Fail-stop: the node has no idea how much of that write landed, so the
            # only honest move is to die and come back from the log.
            self.crash()
            self.on_failure(self.node_id)
            return

        self.unsynced += 1
        if self.unsynced >= SYNC_EVERY:
            self.storage.sync()
            self.unsynced = 0
            # Pay for the fsync by pushing our own next action out. This is the one
            # place the "sync costs virtual ticks" model in sim/disk.py gets settled.
            self._arm(extra=self.storage.take_owed_ticks())

    def _on_timer(self) -> None:
        self.timer = None
        if not self.running:
            return
        for peer in self.peers:
            self.net.send(peer, PING)
        self._arm()

    def _arm(self, *, extra: Tick = 0) -> None:
        if self.timer is not None:
            self.timer.cancel()
        self.timer = self.clock.after(
            self.rng.between(*PING_PERIOD) + extra,
            self._on_timer,
            name="ping_timer",
            actor=self.node_id,
        )

    def _record(self, kind: str, **fields: JsonValue) -> None:
        record: dict[str, JsonValue] = {
            "kind": kind,
            "tick": self.clock.now(),
            "node": self.node_id,
        }
        record.update(fields)
        self.trace.write(record)


class DemoCluster:
    """Wires nodes, network, disks and the fault schedule into one runnable world."""

    def __init__(
        self,
        seed: int,
        *,
        trace: Trace,
        node_count: int = DEFAULT_NODES,
        profile: FaultProfile | None = None,
        max_ticks: Tick = DEFAULT_MAX_TICKS,
    ) -> None:
        self.profile = profile if profile is not None else FaultProfile.quiet()
        self.max_ticks = max_ticks

        self.sim: Simulator[DemoNode] = Simulator(seed, trace=trace)
        rng = self.sim.rng
        node_ids = tuple(range(1, node_count + 1))

        # Drawn before anything runs, and printed in the header. Everything the run
        # does afterwards is a consequence of these two lines.
        self.schedule = generate_schedule(rng, self.profile, node_ids, max_ticks)
        self.sim.header.update(
            {
                "demo": "chatter",
                "profile": self.profile.name,
                "network": dict(self.profile.network.describe()),
                "faults": dict(self.schedule.describe()),
            }
        )

        self.network: SimNetwork[str] = SimNetwork(
            rng, self.sim.clock, trace, faults=self.profile.network
        )

        for node_id in node_ids:
            peers = tuple(peer for peer in node_ids if peer != node_id)
            storage = SimStorage(rng, faults=self.profile.disk)
            link = self.network.register(node_id, self._deliver_to(node_id))
            node = DemoNode(
                node_id=node_id,
                peers=peers,
                clock=self.sim.clock,
                net=link,
                storage=storage,
                rng=rng,
                trace=trace,
                on_failure=self._on_self_crash,
            )
            self.sim.register(node_id, node)

        self.injector = FaultInjector(
            clock=self.sim.clock,
            network=self.network,
            nodes=dict(self.sim.nodes),
            schedule=self.schedule,
        )

    def _deliver_to(self, node_id: NodeId) -> Callable[[NodeId, str], None]:
        def deliver(frm: NodeId, msg: str) -> None:
            self.sim.nodes[node_id].on_message(frm, msg)

        return deliver

    def _on_self_crash(self, node_id: NodeId) -> None:
        """A node died on its own -- a failed disk write, not a scheduled outage.

        Tell the network it is gone (otherwise everyone keeps talking into a corpse)
        and arrange for it to come back, the way a supervisor or systemd would.
        """
        self.network.set_state(node_id, NodeState.CRASHED)
        self.sim.clock.after(
            RESTART_DELAY,
            partial(self._restart, node_id),
            name="node.supervise",
            actor=node_id,
        )

    def _restart(self, node_id: NodeId) -> None:
        # The fault schedule may have restarted it first; that is not a conflict, it is
        # just two supervisors reaching the same conclusion.
        if self.network.state(node_id) is not NodeState.CRASHED:
            return
        self.sim.nodes[node_id].restart()
        self.network.set_state(node_id, NodeState.RUNNING)

    def start(self) -> None:
        for node_id in self.sim.node_ids():
            self.sim.nodes[node_id].start()
        self.injector.arm()

    def run(self) -> RunResult:
        self.start()
        return self.sim.run(self.max_ticks)


def run_demo(
    seed: int,
    *,
    trace: Trace,
    node_count: int = DEFAULT_NODES,
    profile: FaultProfile | None = None,
    max_ticks: Tick = DEFAULT_MAX_TICKS,
) -> tuple[DemoCluster, RunResult]:
    cluster = DemoCluster(
        seed,
        trace=trace,
        node_count=node_count,
        profile=profile,
        max_ticks=max_ticks,
    )
    return cluster, cluster.run()


def profile_by_name(name: str) -> FaultProfile:
    try:
        return PROFILES[name]
    except KeyError:
        raise ValueError(f"unknown fault profile: {name!r}") from None
