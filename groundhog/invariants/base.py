"""The checker protocol, the registry, and what a violation looks like.

    "The invariants are the test. Fault injection only applies pressure." -- spec §3

Worth being precise about what that means. The simulator can run a million universes of
partitions, crashes and torn writes, and if nobody wrote down *a committed value must
never disappear*, every one of them passes and the data still goes missing. The faults
find the interleaving; the invariants are the only thing that notices it mattered.

Which is why every checker in this package has a test that deliberately constructs a
violation and proves the checker fires. A checker that has never caught anything is not
known to work -- it is only known to run.

Cost
----
Checkers run after every event, which for a Raft run is thousands of times. They are
written to be cheap and to bail out early, and the registry takes a `stride` so a sweep
can check every N events instead when throughput matters more than precision. Default 1:
correctness first, and the moment you check less often, the seed that fails stops being
the seed that reproduces.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from groundhog.invariants.history import ClientHistory
from groundhog.raft.node import RaftNode, Role
from groundhog.types import JsonValue, NodeId, Tick


@dataclass(frozen=True, slots=True)
class ClusterView:
    """Everything a checker is allowed to look at, as it stands right now."""

    tick: Tick
    nodes: Mapping[NodeId, RaftNode]
    history: ClientHistory

    def sorted_nodes(self) -> list[RaftNode]:
        """Never iterate the mapping directly -- rule 3, and a violation report that
        names a different node depending on dict order is not reproducible."""
        return [self.nodes[node_id] for node_id in sorted(self.nodes)]

    def leaders(self) -> list[RaftNode]:
        return [node for node in self.sorted_nodes() if node.running and node.role is Role.LEADER]


@dataclass(frozen=True, slots=True)
class Violation:
    """A safety property that stopped being true, and how to see it again."""

    checker: str
    seed: int
    tick: Tick
    detail: str

    def describe(self) -> str:
        return f"[{self.checker}] seed {self.seed} tick {self.tick}: {self.detail}"

    def reproduce(self, *, faults: str = "aggressive") -> str:
        return (
            f"groundhog raft --seed {self.seed} --faults {faults} "
            f"--trace violation-{self.seed}.jsonl"
        )

    def as_record(self) -> dict[str, JsonValue]:
        return {
            "kind": "invariant.violated",
            "checker": self.checker,
            "seed": self.seed,
            "tick": self.tick,
            "detail": self.detail,
        }


class InvariantViolated(Exception):  # noqa: N818
    """Raised out of the run loop when a checker fires and fail-fast is on.

    Not `...Error`: this is not an error in the program, it is the program working. The
    name is what it reads as at the call site, and `except InvariantViolated` says
    exactly what happened.
    """

    def __init__(self, violation: Violation) -> None:
        super().__init__(violation.describe())
        self.violation = violation


class Checker(Protocol):
    """One safety property, watched over the whole run.

    Checkers are stateful on purpose. "At most one leader per term" and "a committed
    entry survives into every future leader" are not questions about a single instant --
    they are questions about history, and a checker that only sees the present cannot
    answer them.
    """

    name: str

    def observe(self, view: ClusterView) -> str | None:
        """Look at the cluster. Return a description of what is wrong, or None."""
        ...


class Invariants:
    """Runs every checker after every event and remembers what broke."""

    def __init__(
        self,
        checkers: Sequence[Checker],
        *,
        seed: int,
        stride: int = 1,
        stop_on_violation: bool = True,
    ) -> None:
        if stride < 1:
            raise ValueError(f"stride must be at least 1, got {stride}")
        self.checkers = list(checkers)
        self.seed = seed
        self.stride = stride
        self.stop_on_violation = stop_on_violation
        self.violations: list[Violation] = []
        self.observations = 0

    def observe(self, view: ClusterView) -> None:
        self.observations += 1
        if self.observations % self.stride:
            return

        for checker in self.checkers:
            problem = checker.observe(view)
            if problem is None:
                continue
            violation = Violation(
                checker=checker.name,
                seed=self.seed,
                tick=view.tick,
                detail=problem,
            )
            self.violations.append(violation)
            if self.stop_on_violation:
                raise InvariantViolated(violation)

    @property
    def ok(self) -> bool:
        return not self.violations
