"""Message passing, behind an interface.

The last of the three handles from spec §5. A node never opens a socket; it hands a
message to its `Network` and forgets about it.

Two things about this signature matter more than they look:

**There is no `from`.** Each node holds a handle bound to itself, so it cannot forge a
sender. In the simulator that is a `NodeLink`; in M9 it will be a real socket that knows
which process it belongs to.

**`send` returns nothing and cannot fail.** No return code, no exception, no delivery
receipt. That is not laziness -- it is the actual guarantee a network gives you. A
successful `send` means the bytes left the building; it says nothing about whether
anyone received them. Any interface that let a node distinguish "delivered" from "lost"
would be a lie, and Raft would end up depending on the lie.
"""

from typing import Protocol, TypeVar

from groundhog.types import NodeId

#: Contravariant: a `Network` that can send any message can stand in for one that sends
#: only votes. Messages only ever go *into* `send`.
Msg_contra = TypeVar("Msg_contra", contravariant=True)


class Network(Protocol[Msg_contra]):
    def send(self, to: NodeId, msg: Msg_contra) -> None:
        """Try to deliver `msg` to `to`. Best effort, and that is all."""
        ...
