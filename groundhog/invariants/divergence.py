"""Do the copies agree?

The weakest interesting question you can ask a replicated system, and the one rung 3 is
built around. No terms, no commit indices, no vocabulary -- just three dictionaries that
are supposed to be the same dictionary.

**When to ask it matters.** Asking after every event would fire constantly and mean
nothing: replication is asynchronous, so a write that has reached the primary and not
yet the backups is a temporary disagreement, not a bug. The question only has an answer
once the world is quiet -- every message delivered, every node up, nothing in flight.
That is why `NaiveCluster` runs to quiescence before it checks.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from groundhog.kv import KvStore
from groundhog.types import NodeId


@dataclass(frozen=True, slots=True)
class Divergence:
    """One key that three replicas do not agree about."""

    key: str
    #: `(node, value)` sorted by node. `None` means the key is absent there.
    values: tuple[tuple[NodeId, str | None], ...]

    def describe(self) -> str:
        seen = ", ".join(f"n{node}={value!r}" for node, value in self.values)
        return f"key {self.key!r}: {seen}"


class DivergenceChecker:
    """Compares replicas against each other. Not against the truth -- see
    `naive/client.py` for that, which is a strictly stronger question: replicas can
    agree perfectly on a value the client was never promised."""

    def __init__(self, stores: Mapping[NodeId, KvStore]) -> None:
        if len(stores) < 2:
            raise ValueError("nothing to compare with fewer than two replicas")
        self.stores = stores

    def check(self) -> list[Divergence]:
        divergences: list[Divergence] = []
        for key in self._all_keys():
            values = tuple(
                (node_id, self.stores[node_id].get(key)) for node_id in sorted(self.stores)
            )
            first = values[0][1]
            if any(value != first for _, value in values):
                divergences.append(Divergence(key=key, values=values))
        return divergences

    def _all_keys(self) -> list[str]:
        # A dict, not a set: rule 3. Insertion order is discarded by the sort anyway,
        # but the habit is the point.
        seen: dict[str, None] = {}
        for node_id in sorted(self.stores):
            for key in self.stores[node_id].all_keys():
                seen[key] = None
        return sorted(seen)
