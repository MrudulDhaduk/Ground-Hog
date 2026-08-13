"""The scalar types the whole system is written in.

These are plain aliases, not `typing.NewType`. That is a deliberate call:

- `NewType` would catch argument-order slips such as `term_at(some_term)`, which is a
  genuine Raft bug class. But it would *not* catch the mistakes that actually bite --
  `if entry.term >= last_index` type-checks either way, because every `NewType` of `int`
  is still an `int` under comparison.
- The cost lands entirely on the arithmetic-heavy code (`Term(term + 1)`,
  `Index(last + 1)`), which is exactly the hand-written Raft core. Fighting the type
  checker while learning the algorithm is a bad trade.

So: these names are documentation for the reader, not a proof for the compiler.
"""

from typing import Final, TypeAlias

#: Identity of a node in the cluster. Never arithmetic, only compared and ordered.
NodeId: TypeAlias = int

#: Raft term. Monotonically increasing, per Figure 2.
Term: TypeAlias = int

#: 1-based index into the replicated log. Index 0 means "before the first entry".
Index: TypeAlias = int

#: Virtual time, in **integer microseconds**.
#:
#: Determinism rule 4: no `float`, no `datetime`, no `timedelta`. Floats accumulate
#: rounding differences across platforms; ints do not.
Tick: TypeAlias = int

MICROSECOND: Final[Tick] = 1
MILLISECOND: Final[Tick] = 1_000 * MICROSECOND
SECOND: Final[Tick] = 1_000 * MILLISECOND
