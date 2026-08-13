"""The client, and the only thing it is entitled to believe.

A client that got back "saved" has been given a promise. It does not know or care how
many machines hold the data, which one is the primary, or whether anything was synced.
Its entire model of the system is: *I asked, you said yes, therefore it is there.*

`expected_state()` is that promise turned into an assertion. For every key, the last
write the client was told succeeded is what the key must contain, everywhere, forever.
Nothing else is a fair expectation -- a write that got no answer may or may not have
landed, and holding the system to those would be inventing requirements.

This is the embryo of the state-machine-safety invariant in M6. It is worth noticing how
little it needed: no consensus vocabulary at all, just a list of things somebody was
promised.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from groundhog.clock import Clock
from groundhog.kv import PUT, Command, KvStore
from groundhog.sim.rng import Rng
from groundhog.sim.trace import Trace
from groundhog.types import MILLISECOND, JsonValue, NodeId, Tick

#: Gap between client writes.
#:
#: Deliberately *smaller* than the spread of network latency. Two writes to the same key
#: are `keys` requests apart, so if the gap between requests were larger than the range
#: of possible delays, the second could never overtake the first and reordering would be
#: unreachable no matter how many seeds you ran. Getting this wrong does not produce a
#: failing test -- it produces a harness that quietly reports everything is fine.
REQUEST_GAP: tuple[Tick, Tick] = (1 * MILLISECOND, 5 * MILLISECOND)


class Submit(Protocol):
    """However a write reaches the primary. In M4 it is a direct call that returns
    immediately, which is exactly the naivety being demonstrated."""

    def __call__(self, command: Command) -> bool: ...


@dataclass(frozen=True, slots=True)
class LostWrite:
    """A value the client was promised, that a replica does not have."""

    node: NodeId
    key: str
    promised: str | None
    found: str | None

    def describe(self) -> str:
        return f"n{self.node} key {self.key!r}: promised {self.promised!r}, found {self.found!r}"


def expected_state(acked: list[Command]) -> dict[str, str | None]:
    """Last acknowledged write per key wins. `None` means "must be absent"."""
    state: dict[str, str | None] = {}
    for command in acked:
        state[command.key] = command.value if command.op == PUT else None
    return state


def lost_writes(acked: list[Command], stores: Mapping[NodeId, KvStore]) -> list[LostWrite]:
    expected = expected_state(acked)
    lost: list[LostWrite] = []
    for node_id in sorted(stores):
        for key in sorted(expected):
            found = stores[node_id].get(key)
            if found != expected[key]:
                lost.append(LostWrite(node=node_id, key=key, promised=expected[key], found=found))
    return lost


class Client:
    """Fires writes at the primary on a timer and remembers what it was promised."""

    def __init__(
        self,
        clock: Clock,
        rng: Rng,
        trace: Trace,
        *,
        submit: "Submit",
        writes: int,
        keys: int,
    ) -> None:
        self.clock = clock
        self.rng = rng
        self.trace = trace
        self.submit = submit
        self.writes = writes
        self.keys = keys

        self.issued = 0
        self.acked: list[Command] = []
        self.refused = 0

    def start(self) -> None:
        self._arm()

    def _arm(self) -> None:
        self.clock.after(self.rng.between(*REQUEST_GAP), self._issue, name="client.request")

    def _issue(self) -> None:
        if self.issued >= self.writes:
            return
        number = self.issued
        self.issued += 1

        # A small key space on purpose: overwrites are where order stops being an
        # academic concern and starts changing the answer.
        key = f"k{number % self.keys}"
        command = Command.delete(key) if number % 7 == 6 else Command.put(key, f"v{number}")

        accepted = self.submit(command)
        if accepted:
            self.acked.append(command)
        else:
            self.refused += 1

        record: dict[str, JsonValue] = {
            "kind": "client.request",
            "tick": self.clock.now(),
            "command": command.describe(),
            "acked": accepted,
        }
        self.trace.write(record)
        self._arm()

    def window(self) -> Tick:
        """A generous upper bound on how long the client will keep writing."""
        return self.writes * REQUEST_GAP[1]
