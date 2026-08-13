"""The single source of randomness.

Determinism rule 1: one `Rng` object, created from the seed, passed explicitly to
everything that needs a choice. Never `import random` anywhere else -- the module-level
`random` functions share one global generator with every library in the process, so a
library that draws a number would silently shift our stream.

Why a seeded PRNG is safe to build on: CPython's Mersenne Twister is a *specified*
algorithm, not an implementation detail. `Random(847392).getrandbits(32)` returns the
same value on Windows and Linux, on CPython 3.9 and 3.13.

`getrandbits()` and `random()` are the parts of that contract the docs actually make.
The convenience wrappers on top of them are not: `randrange` has changed its internals
more than once, and `randint`/`choice` are defined in terms of it. So every method below
is built from `getrandbits` and nothing else, and `test_rng.py` pins the first draws of
seed 4471 as literal values -- if a Python upgrade ever changes the stream, that test
fails instead of a million-seed sweep quietly covering a different million seeds.
"""

import random
from collections.abc import Sequence
from typing import TypeVar

T = TypeVar("T")


class Rng:
    """A seeded generator of integers. Deliberately small.

    Every method consumes a countable number of *logical* draws, tracked in `calls`.
    A trace records that counter next to each event, which makes divergence between two
    runs easy to localise: the first line where the counters differ is where the two
    universes split.
    """

    __slots__ = ("_random", "calls", "seed")

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.calls = 0
        self._random = random.Random(seed)

    def __repr__(self) -> str:
        return f"Rng(seed={self.seed}, calls={self.calls})"

    def below(self, n: int) -> int:
        """A uniform integer in `[0, n)`.

        Rejection sampling on `getrandbits`, which is the same algorithm CPython uses
        internally, written out here so it cannot drift with the interpreter.
        """
        if n <= 0:
            raise ValueError(f"below(n) needs n >= 1, got {n}")
        self.calls += 1
        if n == 1:
            return 0
        bits = (n - 1).bit_length()
        while True:
            candidate = self._random.getrandbits(bits)
            if candidate < n:
                return candidate

    def between(self, low: int, high: int) -> int:
        """A uniform integer in `[low, high]`. **Both ends inclusive.**"""
        if high < low:
            raise ValueError(f"between(low, high) needs low <= high, got {low}, {high}")
        return low + self.below(high - low + 1)

    def chance(self, percent: int) -> bool:
        """True `percent` times in 100.

        Consumes a draw even at 0 or 100. That is on purpose: turning a fault
        probability down to zero then keeps the rest of the stream aligned, so a run
        stays comparable to the run that had the fault enabled.
        """
        if not 0 <= percent <= 100:
            raise ValueError(f"chance(percent) needs 0 <= percent <= 100, got {percent}")
        return self.below(100) < percent

    def pick(self, items: Sequence[T]) -> T:
        """A uniform element of `items`.

        Takes a `Sequence`, never a set: rule 3. An unordered container has no stable
        n-th element, so picking from one is not reproducible.
        """
        if not items:
            raise ValueError("pick() needs a non-empty sequence")
        return items[self.below(len(items))]
