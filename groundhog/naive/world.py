"""Wires the naive replicator into the simulator and asks the only question worth asking.

The check happens at **quiescence**, not continuously. Once the client has stopped
writing, every message has been delivered or lost, and every node is back up, there is
no longer any legitimate reason for the three copies to differ. Before that point,
disagreement is just replication in progress.

The run therefore ends when the queue empties. If it ever stops for `max_ticks` instead,
the result is not trustworthy and says so -- something was still in flight and the
comparison would be measuring the clock, not the code.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial

from groundhog.invariants.divergence import Divergence, DivergenceChecker
from groundhog.kv import Command, KvStore
from groundhog.naive.client import Client, LostWrite, lost_writes
from groundhog.naive.replicator import NaiveReplicator, Replicate
from groundhog.sim.disk import SimStorage
from groundhog.sim.faults import FaultInjector, FaultProfile, generate_schedule
from groundhog.sim.net import NodeState, SimNetwork
from groundhog.sim.trace import Trace
from groundhog.sim.world import STOP_IDLE, RunResult, Simulator
from groundhog.types import SECOND, JsonValue, NodeId, Tick

PRIMARY: NodeId = 1
NODE_IDS: tuple[NodeId, ...] = (1, 2, 3)

#: Quiet time after the last scheduled disturbance, so every crashed node is back and
#: every delayed message has landed before anyone is asked to agree about anything.
SETTLE: Tick = 3 * SECOND

DEFAULT_WRITES = 40
DEFAULT_KEYS = 3


@dataclass(frozen=True, slots=True)
class NaiveResult:
    seed: int
    profile: str
    issued: int
    acked: int
    refused: int
    quiescent: bool
    stores: Mapping[NodeId, Mapping[str, str]]
    divergences: tuple[Divergence, ...]
    lost: tuple[LostWrite, ...]

    @property
    def agrees(self) -> bool:
        return not self.divergences

    @property
    def kept_its_promises(self) -> bool:
        return not self.lost

    def summary(self) -> str:
        verdict = "OK" if self.agrees and self.kept_its_promises else "BROKEN"
        return (
            f"seed {self.seed}  faults {self.profile}  {verdict}  "
            f"acked {self.acked}/{self.issued}  "
            f"divergent_keys {len(self.divergences)}  lost_writes {len(self.lost)}"
        )


class NaiveCluster:
    def __init__(
        self,
        seed: int,
        *,
        trace: Trace,
        profile: FaultProfile,
        writes: int = DEFAULT_WRITES,
        keys: int = DEFAULT_KEYS,
    ) -> None:
        self.profile = profile
        self.sim: Simulator[NaiveReplicator] = Simulator(seed, trace=trace)
        rng = self.sim.rng

        self.client = Client(
            self.sim.clock,
            rng,
            trace,
            submit=self._submit,
            writes=writes,
            keys=keys,
        )

        # Faults only happen while the client is writing. After that the world is left
        # alone to settle, which is what makes the comparison at the end fair.
        self.schedule = generate_schedule(rng, profile, NODE_IDS, self.client.window())
        self.max_ticks = self._quiet_by() + SETTLE

        self.sim.header.update(
            {
                "world": "naive",
                "profile": profile.name,
                "primary": PRIMARY,
                "writes": writes,
                "keys": keys,
                "network": dict(profile.network.describe()),
                "faults": dict(self.schedule.describe()),
            }
        )

        self.network: SimNetwork[Replicate] = SimNetwork(
            rng,
            self.sim.clock,
            trace,
            faults=profile.network,
            describe=lambda msg: msg.describe(),
        )

        for node_id in NODE_IDS:
            peers = tuple(peer for peer in NODE_IDS if peer != node_id)
            link = self.network.register(node_id, self._deliver_to(node_id))
            self.sim.register(
                node_id,
                NaiveReplicator(
                    node_id=node_id,
                    primary_id=PRIMARY,
                    peers=peers,
                    clock=self.sim.clock,
                    net=link,
                    storage=SimStorage(rng, faults=profile.disk),
                    trace=trace,
                ),
            )

        self.injector = FaultInjector(
            clock=self.sim.clock,
            network=self.network,
            nodes=dict(self.sim.nodes),
            schedule=self.schedule,
        )

    # -- wiring ---------------------------------------------------------------

    def _submit(self, command: Command) -> bool:
        return self.sim.nodes[PRIMARY].on_client_request(command)

    def _deliver_to(self, node_id: NodeId) -> "partial[None]":
        return partial(self._receive, node_id)

    def _receive(self, node_id: NodeId, frm: NodeId, msg: Replicate) -> None:
        self.sim.nodes[node_id].on_message(frm, msg)

    def _quiet_by(self) -> Tick:
        """When the last scheduled disturbance is over."""
        ends = [self.client.window()]
        ends += [episode.at + episode.duration for episode in self.schedule.partitions]
        ends += [episode.at + episode.duration for episode in self.schedule.outages]
        return max(ends)

    # -- running --------------------------------------------------------------

    def run(self) -> NaiveResult:
        self.client.start()
        self.injector.arm()
        outcome = self.sim.run(self.max_ticks)
        return self._verdict(outcome)

    def _verdict(self, outcome: RunResult) -> NaiveResult:
        stores: dict[NodeId, KvStore] = {
            node_id: self.sim.nodes[node_id].kv for node_id in NODE_IDS
        }
        everyone_up = all(self.network.state(node_id) is NodeState.RUNNING for node_id in NODE_IDS)
        quiescent = outcome.stop_reason == STOP_IDLE and everyone_up

        divergences = tuple(DivergenceChecker(stores).check())
        lost = tuple(lost_writes(self.client.acked, stores))

        result = NaiveResult(
            seed=outcome.seed,
            profile=self.profile.name,
            issued=self.client.issued,
            acked=len(self.client.acked),
            refused=self.client.refused,
            quiescent=quiescent,
            stores={node_id: stores[node_id].snapshot() for node_id in NODE_IDS},
            divergences=divergences,
            lost=lost,
        )

        verdict: dict[str, JsonValue] = {
            "kind": "check.divergence",
            "tick": self.sim.clock.now(),
            "quiescent": quiescent,
            "divergent_keys": [d.key for d in divergences],
            "lost_writes": len(lost),
            "stores": {str(node_id): dict(result.stores[node_id]) for node_id in NODE_IDS},
        }
        self.sim.trace.write(verdict)
        return result


def run_naive(
    seed: int,
    *,
    trace: Trace,
    profile: FaultProfile,
    writes: int = DEFAULT_WRITES,
    keys: int = DEFAULT_KEYS,
) -> NaiveResult:
    return NaiveCluster(seed, trace=trace, profile=profile, writes=writes, keys=keys).run()
