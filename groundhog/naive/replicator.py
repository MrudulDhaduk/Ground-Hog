"""Rung 3: replication done wrong, on purpose.

Written by Claude at the user's direction, per spec §5's suggestion to let the fault
injector loose on generated code. The four decisions the assignment asks about are
recorded at the bottom of this docstring; `notes/rung3.md` has the failures they cause.

The assignment
==============

Three nodes. Node 1 is the primary, forever -- there are no elections because there is
no concept of a term to elect anyone into. Nodes 2 and 3 are backups. A client sends a
write to the primary and the primary says "saved".

The rules, and they are all *negative*:

- **No quorum.** Do not count acknowledgements. Do not wait for any.
- **No terms, no elections, no leader.** Node 1 is the primary because it says so.
- **Ack immediately.** `on_client_request` returns `True` before the backups have heard
  anything at all. This is the whole point: the client is told the write is safe at a
  moment when exactly one machine has it.

What each method has to do
==========================

`on_client_request(command)`
    Only the primary serves writes. Apply the command to `self.kv`, put it in the log,
    tell both backups, and return `True`. Return `False` if this node is not the primary
    or is not running -- a crashed node cannot promise anything.

`on_message(frm, msg)`
    A backup got a `Replicate`. Apply it. That is all there is; there is nobody to reply
    to, because nobody is listening for a reply.

`crash()`
    The process died. Volatile state goes: set `running` to `False` and throw away
    `self.kv` (build a fresh one). Crash the disk too -- `self.storage.crash()` decides
    what actually reached the platter.

`restart()`
    Come back. `self.storage.restart()` first (it discards a torn tail), then rebuild
    `self.kv` from what survived, then set `running` back to `True`.

The four decisions, and which way this file went
================================================

1. **Ack before or after `sync()`?** -- **Before.** "Ack the client immediately" is the
   assignment, and it is also what a system optimising for latency actually does.
   Syncing is batched: every `SYNC_EVERY` writes. So there is always a window where the
   client has been told yes and nothing is durable anywhere.
2. **Log first or apply first?** -- **Log first.** It costs nothing here, because there
   is no consistency check to fail and no way to reject a command. It will matter in
   Raft, where an entry has to be durable before anyone is told it exists.
3. **Do backups persist?** -- **Yes**, on the same batched-sync path as the primary. A
   backup that only held things in memory would make every crash a total loss and the
   divergences would all look the same.
4. **Does a restarted primary re-send what the backups might have missed?** -- **No.**
   Fire and forget means forget. There is no `matchIndex`, no retry, no reconciliation.
   Nothing in this design ever discovers that a backup is behind.

Nothing here is a bug. Every one of these is a defensible engineering choice, and the
system built out of them loses acknowledged data anyway. That is the rung-3 lesson: the
failure is not in any single decision, it is in the absence of a rule tying them
together.

How to find the failures
========================

    groundhog naive --seed 4471 --faults perfect     # everything agrees
    groundhog naive --seed 4471 --faults quiet       # variable latency and nothing else
    groundhog naive --scan 0:2000 --faults quiet     # find seeds where they do not agree
    groundhog naive --scan 0:2000 --faults aggressive

Start with `perfect` and work down. The interesting discovery is how little it takes:
`quiet` has no drops, no partitions, no crashes and no disk faults. The only thing it
does is let one message take longer than another.
"""

from dataclasses import dataclass
from typing import Final

from groundhog.clock import Clock
from groundhog.kv import Command, KvStore, encode_command
from groundhog.network import Network
from groundhog.sim.disk import SimStorage
from groundhog.sim.trace import Trace
from groundhog.storage import DiskError
from groundhog.types import JsonValue, NodeId

IMPLEMENTED = True

#: Writes between `sync()` calls. Group commit, as every real database does it -- and
#: the reason there is always a window of acknowledged-but-not-durable data.
SYNC_EVERY: Final = 4


@dataclass(frozen=True, slots=True)
class Replicate:
    """The only message this system has. One way, no reply, no acknowledgement."""

    command: Command

    def describe(self) -> str:
        return f"replicate {self.command.describe()}"


class NaiveReplicator:
    """One primary, two backups, and a promise it cannot keep."""

    def __init__(
        self,
        node_id: NodeId,
        primary_id: NodeId,
        peers: tuple[NodeId, ...],
        clock: Clock,
        net: Network[Replicate],
        storage: SimStorage,
        trace: Trace,
    ) -> None:
        self.node_id = node_id
        self.primary_id = primary_id
        self.peers = peers
        self.clock = clock
        self.net = net
        self.storage = storage
        self.trace = trace

        self.kv = KvStore()
        self.running = True
        self.unsynced = 0

    @property
    def is_primary(self) -> bool:
        return self.node_id == self.primary_id

    # -- the replication --------------------------------------------------------

    def on_client_request(self, command: Command) -> bool:
        """Serve a write, and say yes before anyone else has heard of it."""
        if not self.is_primary or not self.running:
            return False

        if not self._write(command):
            return False

        for peer in self.peers:
            self.net.send(peer, Replicate(command))

        # The client is told the write is safe here: one machine has it, possibly not
        # even on disk yet, and the backups have been sent a message that may never
        # arrive. Nothing above this line checked anything.
        self.record("naive.acked", command=command.describe())
        return True

    def on_message(self, frm: NodeId, msg: Replicate) -> None:
        """A backup applies whatever it is handed. There is nobody to answer."""
        if not self.running:
            return
        self._write(msg.command)

    def _write(self, command: Command) -> bool:
        """Log it, apply it, and sync every so often. False if the disk killed us."""
        try:
            self.storage.append([encode_command(command)])
        except DiskError as exc:
            # Fail-stop: no idea how much of that landed, so the only honest move is to
            # die. Nothing restarts this node, which is itself part of the lesson.
            self.record("naive.disk_error", error=str(exc))
            self.crash()
            return False

        self.kv.apply(command)
        self.unsynced += 1
        if self.unsynced >= SYNC_EVERY:
            self.storage.sync()
            self.unsynced = 0
        return True

    # -- lifecycle --------------------------------------------------------------

    def crash(self) -> None:
        """The process died. Volatile state goes; the synced disk stays."""
        if not self.running:
            return
        self.running = False
        self.kv = KvStore()
        self.unsynced = 0
        self.storage.crash()
        self.record("naive.crash")

    def restart(self) -> None:
        """Come back from the log, rebuilding the map from what survived."""
        self.storage.restart()
        self.kv = KvStore.replay(self.storage.read_all())
        self.unsynced = 0
        self.running = True
        self.record("naive.restart", recovered=len(self.kv))

    # -- plumbing you may use -------------------------------------------------

    def record(self, kind: str, **fields: JsonValue) -> None:
        """Put a line in the trace. Costs nothing and makes replay readable."""
        entry: dict[str, JsonValue] = {
            "kind": kind,
            "tick": self.clock.now(),
            "node": self.node_id,
        }
        entry.update(fields)
        self.trace.write(entry)
