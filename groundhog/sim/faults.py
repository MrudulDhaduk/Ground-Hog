"""What goes wrong, when, and to whom -- decided before the run starts.

Two kinds of fault live here and they are generated differently on purpose.

**Per-message faults** (drop, delay, duplicate) are probabilities. They cannot be
scheduled in advance because they attach to messages that do not exist yet.

**Episodes** (partitions, crashes, hangs) *are* scheduled in advance, as a timeline
drawn from the rng before the first event runs, and printed into the trace header.
Two reasons that is worth the extra machinery:

1. You can read the header and know what the universe is going to do to you, before
   spending an hour reading 400 lines of trace to infer it.
2. M7's shrinker needs a fault schedule it can *edit* -- delete an episode, re-run, see
   if the bug survives. A schedule that only exists as a series of coin flips buried in
   the rng stream cannot be edited; a list of dataclasses can.

Episodes never overlap per node: the generator walks each node's timeline forward, so a
node cannot be crashed and hung at once. Partitions likewise are one at a time.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import partial
from typing import Final, Protocol

from groundhog.clock import SimClock
from groundhog.sim.disk import DiskFaults
from groundhog.sim.net import NetworkFaults, NodeState, Partition
from groundhog.sim.rng import Rng
from groundhog.types import MILLISECOND, SECOND, JsonValue, NodeId, Tick

CRASH: Final = "crash"
HANG: Final = "hang"


class NodeLifecycle(Protocol):
    """What the injector needs from a node to be able to kill it.

    Structural, so nothing has to inherit from anything: M3's demo node and M5's Raft
    node satisfy this by having the two methods.
    """

    def crash(self) -> None:
        """Lose all volatile state. The synced disk survives; nothing else does."""
        ...

    def restart(self) -> None:
        """Come back from the log, as a process started fresh would."""
        ...


@dataclass(frozen=True, slots=True)
class PartitionEpisode:
    at: Tick
    duration: Tick
    partition: Partition

    def describe(self) -> Mapping[str, JsonValue]:
        return {"at": self.at, "duration": self.duration, **self.partition.describe()}


@dataclass(frozen=True, slots=True)
class OutageEpisode:
    at: Tick
    duration: Tick
    node: NodeId
    #: `CRASH` or `HANG`.
    kind: str

    def describe(self) -> Mapping[str, JsonValue]:
        return {"at": self.at, "duration": self.duration, "node": self.node, "kind": self.kind}


@dataclass(frozen=True, slots=True)
class FaultSchedule:
    partitions: tuple[PartitionEpisode, ...] = ()
    outages: tuple[OutageEpisode, ...] = ()

    def describe(self) -> Mapping[str, JsonValue]:
        return {
            "partitions": [episode.describe() for episode in self.partitions],
            "outages": [episode.describe() for episode in self.outages],
        }

    def without(self, index: int, *, of: str) -> "FaultSchedule":
        """A copy with one episode removed. This is the shrinker's only lever."""
        if of == "partitions":
            return replace(self, partitions=self.partitions[:index] + self.partitions[index + 1 :])
        if of == "outages":
            return replace(self, outages=self.outages[:index] + self.outages[index + 1 :])
        raise ValueError(f"no such episode kind: {of}")


@dataclass(frozen=True, slots=True)
class FaultProfile:
    """Everything that can go wrong, and how often."""

    name: str = "quiet"
    network: NetworkFaults = field(default_factory=NetworkFaults)
    disk: DiskFaults = field(default_factory=DiskFaults)

    partitions: bool = False
    #: Inclusive gap between the end of one partition and the start of the next.
    partition_gap: tuple[Tick, Tick] = (200 * MILLISECOND, 2 * SECOND)
    partition_duration: tuple[Tick, Tick] = (100 * MILLISECOND, 1 * SECOND)
    one_way_percent: int = 30

    outages: bool = False
    outage_gap: tuple[Tick, Tick] = (300 * MILLISECOND, 3 * SECOND)
    outage_duration: tuple[Tick, Tick] = (50 * MILLISECOND, 800 * MILLISECOND)
    #: Share of outages that are hangs rather than crashes.
    hang_percent: int = 30

    @classmethod
    def perfect(cls) -> "FaultProfile":
        """Nothing goes wrong, and latency is *constant*.

        Not the same as `quiet`. A fixed delay makes the network FIFO, which quietly
        removes reordering -- the one fault that needs no failure at all to happen. It
        is worth having a profile where even that is switched off, if only to have
        something to compare `quiet` against.
        """
        return cls(name="perfect", network=NetworkFaults(latency=(5 * MILLISECOND,) * 2))

    @classmethod
    def quiet(cls) -> "FaultProfile":
        """A network that only takes time -- a varying amount of it.

        No drops, no partitions, no crashes, no disk faults. The only thing that ever
        happens is that one message takes longer than another.
        """
        return cls()

    @classmethod
    def aggressive(cls) -> "FaultProfile":
        return cls(
            name="aggressive",
            network=NetworkFaults(
                latency=(1 * MILLISECOND, 60 * MILLISECOND),
                drop_percent=8,
                duplicate_percent=4,
            ),
            disk=DiskFaults.aggressive(),
            partitions=True,
            outages=True,
        )


PROFILES: Final[Mapping[str, FaultProfile]] = {
    "perfect": FaultProfile.perfect(),
    "quiet": FaultProfile.quiet(),
    "aggressive": FaultProfile.aggressive(),
}


def profile_by_name(name: str) -> FaultProfile:
    try:
        return PROFILES[name]
    except KeyError:
        raise ValueError(f"unknown fault profile: {name!r}") from None


def _split(rng: Rng, node_ids: Sequence[NodeId]) -> tuple[tuple[NodeId, ...], tuple[NodeId, ...]]:
    """Cut the cluster in two, both sides non-empty.

    A partial Fisher-Yates draw rather than a coin flip per node: a coin flip can land
    on "everyone on one side", which is not a partition, and retrying until it does not
    burns a variable number of draws for no reason.
    """
    pool = list(node_ids)
    size = rng.between(1, len(pool) - 1)
    chosen: list[NodeId] = []
    for _ in range(size):
        chosen.append(pool.pop(rng.below(len(pool))))
    return tuple(sorted(chosen)), tuple(sorted(pool))


def generate_schedule(
    rng: Rng,
    profile: FaultProfile,
    node_ids: Sequence[NodeId],
    horizon: Tick,
) -> FaultSchedule:
    """Draw the whole timeline up front. Same seed, same disasters, same order."""
    partitions: list[PartitionEpisode] = []
    if profile.partitions and len(node_ids) >= 2:
        at = rng.between(*profile.partition_gap)
        while at < horizon:
            duration = rng.between(*profile.partition_duration)
            side_a, side_b = _split(rng, node_ids)
            one_way = rng.chance(profile.one_way_percent)
            partitions.append(
                PartitionEpisode(
                    at=at,
                    duration=duration,
                    partition=Partition(side_a=side_a, side_b=side_b, one_way=one_way),
                )
            )
            at += duration + rng.between(*profile.partition_gap)

    outages: list[OutageEpisode] = []
    if profile.outages:
        for node_id in sorted(node_ids):
            at = rng.between(*profile.outage_gap)
            while at < horizon:
                duration = rng.between(*profile.outage_duration)
                kind = HANG if rng.chance(profile.hang_percent) else CRASH
                outages.append(OutageEpisode(at=at, duration=duration, node=node_id, kind=kind))
                at += duration + rng.between(*profile.outage_gap)

    # Sorted by (tick, node) so the header reads as a timeline and two runs of the same
    # seed produce byte-identical headers regardless of how they were generated.
    outages.sort(key=lambda episode: (episode.at, episode.node))
    return FaultSchedule(partitions=tuple(partitions), outages=tuple(outages))


class NetworkControl(Protocol):
    """The parts of the network a fault injector is allowed to touch.

    Narrower than `SimNetwork` on purpose, and not generic in the message type: the
    injector has no business knowing what the cluster is talking about.
    """

    def split(self, partition: Partition) -> None: ...

    def heal(self) -> None: ...

    def set_state(self, node_id: NodeId, state: NodeState) -> None: ...


class FaultInjector:
    """Turns a schedule into events on the queue, and applies them when they fire."""

    def __init__(
        self,
        clock: SimClock,
        network: NetworkControl,
        nodes: Mapping[NodeId, NodeLifecycle],
        schedule: FaultSchedule,
    ) -> None:
        self.clock = clock
        self.network = network
        self.nodes = nodes
        self.schedule = schedule
        #: Every episode that actually fired, in order. `test_faults.py` uses this to
        #: prove each kind of fault is reachable rather than merely configurable.
        self.fired: list[str] = []

    def arm(self) -> None:
        """Put every episode on the event queue. Call once, before `run()`."""
        for split in self.schedule.partitions:
            self._at(split.at, "fault.partition", partial(self._start_partition, split))
            self._at(split.at + split.duration, "fault.heal", self._heal)

        for outage in self.schedule.outages:
            self._at(outage.at, f"fault.{outage.kind}", partial(self._begin, outage))
            self._at(outage.at + outage.duration, "fault.recover", partial(self._end, outage))

    def _at(self, tick: Tick, kind: str, action: Callable[[], None]) -> None:
        self.clock.after(max(0, tick - self.clock.now()), action, name=kind)

    def _start_partition(self, episode: PartitionEpisode) -> None:
        self.fired.append("partition")
        self.network.split(episode.partition)

    def _heal(self) -> None:
        self.fired.append("heal")
        self.network.heal()

    def _begin(self, episode: OutageEpisode) -> None:
        self.fired.append(episode.kind)
        if episode.kind == CRASH:
            self.network.set_state(episode.node, NodeState.CRASHED)
            self.nodes[episode.node].crash()
        else:
            self.network.set_state(episode.node, NodeState.HUNG)

    def _end(self, episode: OutageEpisode) -> None:
        self.fired.append(f"{episode.kind}_end")
        if episode.kind == CRASH:
            self.nodes[episode.node].restart()
        self.network.set_state(episode.node, NodeState.RUNNING)
