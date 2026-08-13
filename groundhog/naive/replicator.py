"""**You write this file.** Everything below `__init__` is yours.

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

The decisions you have to make, and must write down
===================================================

These are not hints, they are the exercise. Each one is a fork, both directions are
defensible, and the direction you pick determines which of the three failures you get.
Record what you chose in `notes/rung3.md`.

1. **Ack before or after `self.storage.sync()`?** Acking first is faster and is what the
   assignment says to do. What does a crash cost you?
2. **Log first, or apply first?** Does it matter here? Will it matter in Raft?
3. **Do backups persist what they receive, or only hold it in memory?**
4. **Does a restarted primary re-send anything it has that the backups might not?**
   (The naive answer is no. Fire and forget means forget.)

How to find the failures
========================

    groundhog naive --seed 4471 --faults perfect     # everything agrees
    groundhog naive --seed 4471 --faults quiet       # variable latency and nothing else
    groundhog naive --scan 0:2000 --faults quiet     # find seeds where they do not agree
    groundhog naive --scan 0:2000 --faults aggressive

Start with `perfect` and work down. The interesting discovery is how little it takes:
`quiet` has no drops, no partitions, no crashes and no disk faults. The only thing it
does is let one message take longer than another.

When you are done, set `IMPLEMENTED = True` below -- `tests/test_naive_replication.py`
skips itself until you do.
"""

from dataclasses import dataclass

from groundhog.clock import Clock
from groundhog.kv import Command, KvStore
from groundhog.network import Network
from groundhog.sim.disk import SimStorage
from groundhog.sim.trace import Trace
from groundhog.types import JsonValue, NodeId

#: Flip this when you have written the four methods below.
IMPLEMENTED = False


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

    @property
    def is_primary(self) -> bool:
        return self.node_id == self.primary_id

    # -- yours from here ------------------------------------------------------

    def on_client_request(self, command: Command) -> bool:
        """Serve a write. Return True if the client is told it was saved."""
        raise NotImplementedError("M4 [Y]: see the module docstring")

    def on_message(self, frm: NodeId, msg: Replicate) -> None:
        """A backup receives a replicated command."""
        raise NotImplementedError("M4 [Y]: see the module docstring")

    def crash(self) -> None:
        """The process died. Keep only what was synced."""
        raise NotImplementedError("M4 [Y]: see the module docstring")

    def restart(self) -> None:
        """Come back from the log."""
        raise NotImplementedError("M4 [Y]: see the module docstring")

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
