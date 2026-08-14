"""A 3-node Raft cluster inside the simulator, with a client that keeps its receipts.

M5's done-when: *a 3-node cluster elects a leader, replicates writes, survives leader
crash, and recovers -- under a clean network first, then under faults.*

Two things here that are not in `naive/world.py`, and both are the point of Raft:

- **The client is on the network.** It sends `ClientRequest` and waits for
  `ClientReply`, which arrives only after the entry is committed and applied. In rung 3
  the client got an answer synchronously, at a moment when one machine had the data.
- **Nobody is the leader by configuration.** The cluster has to elect one, and elect
  another when that one dies.

The client sits at node id 0, outside the cluster. It is never partitioned or crashed --
a real client can be both, but modelling that adds noise to a milestone whose subject is
the cluster. Worth stating rather than discovering: a partitioned *client* is a case
this harness does not cover.

Two more limitations, stated here rather than found later:

- **A healthy Raft cluster is never idle.** The leader heartbeats forever, so unlike
  `naive/world.py` this run always ends at `max_ticks`, and "settled" has to mean
  something else: everyone up, nothing outstanding from the client.
- **There is no client request deduplication.** A client that retries after a timeout
  can have the same command committed twice, so the log can be longer than the number
  of acknowledged writes. Real systems attach a session and a sequence number to make
  writes idempotent; that is exactly-once semantics, which is a layer above consensus
  and out of scope here.
"""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from functools import partial

from groundhog.clock import Clock, Timer
from groundhog.invariants.base import ClusterView, Invariants, InvariantViolated, Violation
from groundhog.invariants.history import ClientHistory
from groundhog.invariants.safety import all_checkers
from groundhog.kv import Command
from groundhog.messages import ClientReply, ClientRequest, Message, RequestVote
from groundhog.network import Network
from groundhog.raft.node import RaftNode, Role
from groundhog.raft.persist import RaftStorage
from groundhog.sim.disk import SimStorage
from groundhog.sim.faults import FaultInjector, FaultProfile, generate_schedule
from groundhog.sim.net import NodeState, SimNetwork
from groundhog.sim.rng import Rng
from groundhog.sim.trace import Trace
from groundhog.sim.world import RunResult, Simulator
from groundhog.types import MILLISECOND, SECOND, Index, JsonValue, NodeId, Tick

CLIENT_ID: NodeId = 0
NODE_IDS: tuple[NodeId, ...] = (1, 2, 3)

#: How long the client waits before assuming a request is lost and trying again.
CLIENT_RETRY: Tick = 400 * MILLISECOND
REQUEST_GAP: tuple[Tick, Tick] = (5 * MILLISECOND, 40 * MILLISECOND)

#: Quiet time after the last disturbance, so an election can finish and replication can
#: catch up before anybody is asked whether the cluster agrees.
SETTLE: Tick = 5 * SECOND

#: How long a supervisor takes to notice a node killed itself and bring it back.
SUPERVISOR_DELAY: Tick = 200 * MILLISECOND

DEFAULT_WRITES = 20
DEFAULT_KEYS = 3


class SimNode:
    """Adapts a `RaftNode` to `NodeLifecycle` by owning the disk it dies with.

    `RaftNode.crash()` deliberately does not touch storage -- see its docstring -- so
    something outside it has to. In M9 that something is the operating system.
    """

    def __init__(self, node: RaftNode, disk: SimStorage) -> None:
        self.node = node
        self.disk = disk

    def crash(self) -> None:
        self.node.crash()
        self.disk.crash()

    def restart(self) -> None:
        self.disk.restart()
        self.node.restart()


class Client:
    """Writes to whichever node it currently believes is the leader, and retries."""

    def __init__(
        self,
        clock: Clock,
        rng: Rng,
        trace: Trace,
        net: Network[Message],
        *,
        writes: int,
        keys: int,
    ) -> None:
        self.clock = clock
        self.rng = rng
        self.trace = trace
        self.net = net
        self.writes = writes
        self.keys = keys

        self.target: NodeId = NODE_IDS[0]
        self.next_request_id = 0
        self.inflight: tuple[int, Command] | None = None
        self.acked: list[Command] = []
        self.attempts = 0
        self.retry_timer: Timer | None = None

    def start(self) -> None:
        self._issue()

    def _issue(self) -> None:
        if len(self.acked) >= self.writes:
            return
        number = self.next_request_id
        self.next_request_id += 1
        key = f"k{len(self.acked) % self.keys}"
        command = Command.put(key, f"v{len(self.acked)}")
        self.inflight = (number, command)
        self._send()

    def _send(self) -> None:
        if self.inflight is None:
            return
        request_id, command = self.inflight
        self.attempts += 1
        self.trace.write(
            {
                "kind": "client.send",
                "tick": self.clock.now(),
                "to": self.target,
                "request": request_id,
                "command": command.describe(),
            }
        )
        self.net.send(self.target, ClientRequest(request_id=request_id, command=command))
        self._arm_retry()

    def _arm_retry(self) -> None:
        if self.retry_timer is not None:
            self.retry_timer.cancel()
        self.retry_timer = self.clock.after(CLIENT_RETRY, self._on_timeout, name="client.retry")

    def _on_timeout(self) -> None:
        """No answer. Try somebody else -- this is how a client finds a new leader."""
        self.retry_timer = None
        if self.inflight is None:
            return
        self.target = NODE_IDS[(NODE_IDS.index(self.target) + 1) % len(NODE_IDS)]
        self._send()

    def on_message(self, frm: NodeId, msg: Message) -> None:
        if not isinstance(msg, ClientReply) or self.inflight is None:
            return
        request_id, command = self.inflight
        if msg.request_id != request_id:
            return  # an answer to a question we already gave up on

        if msg.ok:
            self.acked.append(command)
            self.inflight = None
            if self.retry_timer is not None:
                self.retry_timer.cancel()
                self.retry_timer = None
            self.trace.write(
                {
                    "kind": "client.acked",
                    "tick": self.clock.now(),
                    "request": request_id,
                    "command": command.describe(),
                }
            )
            self.clock.after(self.rng.between(*REQUEST_GAP), self._issue, name="client.next")
            return

        # Refused. The hint may be stale, but it is better than guessing.
        if msg.leader_hint is not None:
            self.target = msg.leader_hint
        else:
            self.target = NODE_IDS[(NODE_IDS.index(self.target) + 1) % len(NODE_IDS)]
        self.clock.after(self.rng.between(*REQUEST_GAP), self._send, name="client.redirect")

    def window(self) -> Tick:
        return self.writes * (REQUEST_GAP[1] + CLIENT_RETRY)


#: What a `--mutate` option can break, deliberately.
#:
#: Mutations are applied to *messages in flight*, never by editing the node's code. Two
#: reasons that is the right place: `raft/node.py` is hand-written and a mutation switch
#: buried in it would be one more thing to reason about while writing consensus; and a
#: message-level mutation is honest about what it removes. Rewriting an incoming
#: RequestVote to claim a maximally up-to-date log makes every voter's §5.4.1 comparison
#: pass -- which is exactly, and only, "the election restriction is not there".
NO_ELECTION_RESTRICTION = "no-election-restriction"
MUTATIONS: tuple[str, ...] = ("none", NO_ELECTION_RESTRICTION)

#: Bigger than any log this simulation will ever produce.
_UNBEATABLE = 1 << 40


@dataclass(frozen=True, slots=True)
class RaftResult:
    seed: int
    profile: str
    acked: int
    requested: int
    attempts: int
    quiescent: bool
    leaders_elected: int
    terms_seen: int
    committed: Mapping[NodeId, Index]
    stores: Mapping[NodeId, Mapping[str, str]]
    violations: tuple[Violation, ...] = ()

    @property
    def safe(self) -> bool:
        return not self.violations

    def summary(self) -> str:
        verdict = "SAFE" if self.safe else f"VIOLATED ({self.violations[0].checker})"
        return (
            f"seed {self.seed}  faults {self.profile}  {verdict}  "
            f"acked {self.acked}/{self.requested}  attempts {self.attempts}  "
            f"max_term {self.terms_seen}"
        )


class RaftCluster:
    def __init__(
        self,
        seed: int,
        *,
        trace: Trace,
        profile: FaultProfile,
        writes: int = DEFAULT_WRITES,
        keys: int = DEFAULT_KEYS,
        check: bool = True,
        stride: int = 1,
        mutate: str = "none",
    ) -> None:
        if mutate not in MUTATIONS:
            raise ValueError(f"unknown mutation: {mutate!r}; try one of {MUTATIONS}")
        self.profile = profile
        self.mutate = mutate
        self.history = ClientHistory()
        self.sim: Simulator[SimNode] = Simulator(seed, trace=trace)
        rng = self.sim.rng

        self.invariants = Invariants(
            all_checkers() if check else [],
            seed=seed,
            stride=stride,
            stop_on_violation=True,
        )
        self.sim.after_event.append(self._check)

        self.network: SimNetwork[Message] = SimNetwork(
            rng,
            self.sim.clock,
            trace,
            faults=profile.network,
            describe=lambda msg: msg.describe(),
        )

        self.client = Client(
            self.sim.clock,
            rng,
            trace,
            self.network.register(CLIENT_ID, self._deliver_to_client),
            writes=writes,
            keys=keys,
        )

        self.schedule = generate_schedule(rng, profile, NODE_IDS, self.client.window())
        self.max_ticks = self._quiet_by() + SETTLE

        self.sim.header.update(
            {
                "world": "raft",
                "profile": profile.name,
                "nodes": list(NODE_IDS),
                "writes": writes,
                "network": dict(profile.network.describe()),
                "faults": dict(self.schedule.describe()),
            }
        )

        for node_id in NODE_IDS:
            peers = tuple(peer for peer in NODE_IDS if peer != node_id)
            disk = SimStorage(rng, faults=profile.disk)
            link = self.network.register(node_id, self._deliver_to(node_id))
            node = RaftNode(
                node_id=node_id,
                peers=peers,
                clock=self.sim.clock,
                net=link,
                storage=RaftStorage(disk),
                rng=rng,
                trace=trace,
                on_failure=self._on_self_crash,
            )
            self.sim.register(node_id, SimNode(node, disk))

        self.injector = FaultInjector(
            clock=self.sim.clock,
            network=self.network,
            nodes=dict(self.sim.nodes),
            schedule=self.schedule,
        )

    # -- wiring ---------------------------------------------------------------

    def node(self, node_id: NodeId) -> RaftNode:
        return self.sim.nodes[node_id].node

    def _deliver_to(self, node_id: NodeId) -> "partial[None]":
        return partial(self._receive, node_id)

    def _receive(self, node_id: NodeId, frm: NodeId, msg: Message) -> None:
        self.node(node_id).on_message(frm, self._mutated(msg))

    def _mutated(self, msg: Message) -> Message:
        """Break one rule on the way in, if asked to."""
        if self.mutate == NO_ELECTION_RESTRICTION and isinstance(msg, RequestVote):
            return replace(msg, last_log_index=_UNBEATABLE, last_log_term=_UNBEATABLE)
        return msg

    def _deliver_to_client(self, frm: NodeId, msg: Message) -> None:
        if isinstance(msg, ClientReply) and msg.ok and self.client.inflight is not None:
            request_id, command = self.client.inflight
            if msg.request_id == request_id:
                self.history.record_ack(self.sim.clock.now(), request_id, command, frm)
        self.client.on_message(frm, msg)

    def _check(self, tick: Tick) -> None:
        self.invariants.observe(
            ClusterView(
                tick=tick,
                nodes={node_id: self.node(node_id) for node_id in NODE_IDS},
                history=self.history,
            )
        )

    def _on_self_crash(self, node_id: NodeId) -> None:
        """A node's disk failed and it took itself down. Play the supervisor."""
        self.network.set_state(node_id, NodeState.CRASHED)
        self.sim.nodes[node_id].disk.crash()
        self.sim.clock.after(
            SUPERVISOR_DELAY,
            partial(self._revive, node_id),
            name="node.supervise",
            actor=node_id,
        )

    def _revive(self, node_id: NodeId) -> None:
        # The fault schedule may have restarted it first; two supervisors reaching the
        # same conclusion is not a conflict.
        if self.network.state(node_id) is not NodeState.CRASHED:
            return
        self.sim.nodes[node_id].restart()
        self.network.set_state(node_id, NodeState.RUNNING)

    def _quiet_by(self) -> Tick:
        ends = [self.client.window()]
        ends += [episode.at + episode.duration for episode in self.schedule.partitions]
        ends += [episode.at + episode.duration for episode in self.schedule.outages]
        return max(ends)

    # -- running --------------------------------------------------------------

    def start(self) -> None:
        for node_id in NODE_IDS:
            self.node(node_id).start()
        self.client.start()
        self.injector.arm()

    def run(self) -> RaftResult:
        self.start()
        try:
            outcome = self.sim.run(self.max_ticks)
        except InvariantViolated as caught:
            # Fail fast: the run stops at the tick the property broke, so the trace ends
            # on the event that did it rather than a thousand events of aftermath.
            self.sim.trace.write(caught.violation.as_record())
            outcome = RunResult(
                seed=self.sim.rng.seed,
                events=0,
                final_tick=self.sim.clock.now(),
                rng_calls=self.sim.rng.calls,
                stop_reason="violation",
            )
        return self._verdict(outcome)

    def leader_ids(self) -> list[NodeId]:
        """Everyone who currently thinks it is the leader. More than one is not
        automatically a bug -- a deposed leader in a partition still believes it -- but
        two in the *same term* is election safety violated. M6 checks that."""
        return [node_id for node_id in NODE_IDS if self.node(node_id).role is Role.LEADER]

    def _verdict(self, outcome: RunResult) -> RaftResult:
        everyone_up = all(self.network.state(node_id) is NodeState.RUNNING for node_id in NODE_IDS)
        nodes = [self.node(node_id) for node_id in NODE_IDS]

        result = RaftResult(
            seed=outcome.seed,
            profile=self.profile.name,
            acked=len(self.client.acked),
            requested=self.client.writes,
            attempts=self.client.attempts,
            # Not `stop_reason == STOP_IDLE`: a live leader heartbeats forever, so this
            # world never runs out of events. Settled means nobody is down and the
            # client is not still waiting for an answer.
            quiescent=everyone_up and self.client.inflight is None,
            leaders_elected=len(self.leader_ids()),
            terms_seen=max(node.current_term for node in nodes),
            committed={node.node_id: node.commit_index for node in nodes},
            stores={node.node_id: node.kv.snapshot() for node in nodes},
            violations=tuple(self.invariants.violations),
        )

        verdict: dict[str, JsonValue] = {
            "kind": "raft.verdict",
            "tick": self.sim.clock.now(),
            "acked": result.acked,
            "max_term": result.terms_seen,
            "commit": {str(k): v for k, v in result.committed.items()},
            "stores": {str(k): dict(v) for k, v in result.stores.items()},
        }
        self.sim.trace.write(verdict)
        return result


def run_raft(
    seed: int,
    *,
    trace: Trace,
    profile: FaultProfile,
    writes: int = DEFAULT_WRITES,
    keys: int = DEFAULT_KEYS,
    check: bool = True,
    stride: int = 1,
    mutate: str = "none",
) -> RaftResult:
    return RaftCluster(
        seed,
        trace=trace,
        profile=profile,
        writes=writes,
        keys=keys,
        check=check,
        stride=stride,
        mutate=mutate,
    ).run()
