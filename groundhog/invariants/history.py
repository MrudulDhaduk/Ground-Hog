"""What the client was told, which is the only thing the system actually owes anyone.

A distributed database has no obligation to any particular internal arrangement. It has
exactly one obligation, and it is to the outside: *if you told somebody a write
succeeded, that write happened, and it stays happened.*

Everything in `safety.py` except `StateMachineSafety` is an internal consistency check --
useful, but a system can satisfy all of them and still lose your data, because they only
say the replicas agree with each other. This file is what makes "agree about the right
thing" expressible.

Only acknowledged writes go in here. A request that got no answer may or may not have
landed, and both outcomes are legal -- holding the system to writes it never confirmed
would be inventing requirements it never took on.
"""

from dataclasses import dataclass, field

from groundhog.kv import PUT, Command
from groundhog.types import Index, NodeId, Tick


@dataclass(frozen=True, slots=True)
class Ack:
    """One promise, and when it was made."""

    tick: Tick
    request_id: int
    command: Command
    #: Which node answered. Useful in a violation report; it is often already dead.
    by: NodeId

    def describe(self) -> str:
        return f"#{self.request_id} {self.command.describe()} acked by n{self.by} at {self.tick}"


@dataclass(slots=True)
class ClientHistory:
    acks: list[Ack] = field(default_factory=list)

    def record_ack(self, tick: Tick, request_id: int, command: Command, by: NodeId) -> None:
        self.acks.append(Ack(tick=tick, request_id=request_id, command=command, by=by))

    def commands(self) -> list[Command]:
        return [ack.command for ack in self.acks]

    def expected_state(self) -> dict[str, str | None]:
        """Last acknowledged write per key. `None` means the key must be absent.

        Only meaningful once the world is quiet: while writes are still in flight, a
        replica legitimately holds an older acknowledged value. Lag is not loss.
        """
        state: dict[str, str | None] = {}
        for ack in self.acks:
            state[ack.command.key] = ack.command.value if ack.command.op == PUT else None
        return state

    def __len__(self) -> int:
        return len(self.acks)


@dataclass(frozen=True, slots=True)
class AppliedEntry:
    """The first command anybody applied at a given log index.

    Once one server has applied an entry at index `i`, no other server may ever apply a
    different one there. That sentence is State Machine Safety, and this is how it gets
    remembered.
    """

    index: Index
    term: int
    command: Command
    by: NodeId

    def describe(self) -> str:
        return (
            f"index {self.index} term {self.term} {self.command.describe()} (first on n{self.by})"
        )
