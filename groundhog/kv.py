"""The state machine: a dictionary, and the commands that change it.

Deliberately the most boring file in the project. That is the point -- the interesting
part of a replicated database is never the database. `put`, `get`, `delete`, applied in
order. Everything hard is about *which* order, and which entries are safe to apply at
all, and that lives everywhere else.

Two properties this file must have, because the layers above assume them:

- **Deterministic.** `snapshot()` is built in sorted key order, so two stores holding
  the same data serialise identically no matter what order they were filled in.
- **Total.** Applying a command never fails and never depends on what came before.
  Deleting a missing key is fine. A state machine that can reject an entry is a state
  machine two replicas can disagree about, which would move a consensus problem into
  the one place that must not have one.
"""

import struct
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

PUT: Final = "put"
DELETE: Final = "delete"

_CODE_BY_OP: Final[dict[str, int]] = {PUT: 1, DELETE: 2}
_OP_BY_CODE: Final[dict[int, str]] = {code: op for op, code in _CODE_BY_OP.items()}

#: op code, key length, value length. Big-endian, like the WAL framing.
_HEADER: Final = struct.Struct(">BHI")


@dataclass(frozen=True, slots=True)
class Command:
    op: str
    key: str
    value: str = ""

    @classmethod
    def put(cls, key: str, value: str) -> "Command":
        return cls(op=PUT, key=key, value=value)

    @classmethod
    def delete(cls, key: str) -> "Command":
        return cls(op=DELETE, key=key)

    def describe(self) -> str:
        return f"{self.key}={self.value}" if self.op == PUT else f"del {self.key}"


def encode_command(command: Command) -> bytes:
    try:
        code = _CODE_BY_OP[command.op]
    except KeyError:
        raise ValueError(f"unknown operation: {command.op!r}") from None
    key = command.key.encode("utf-8")
    value = command.value.encode("utf-8")
    return _HEADER.pack(code, len(key), len(value)) + key + value


def decode_command(data: bytes) -> Command:
    code, key_length, value_length = _HEADER.unpack_from(data, 0)
    start = _HEADER.size
    middle = start + key_length
    end = middle + value_length
    if end != len(data):
        raise ValueError(f"command is {len(data)} bytes, header claims {end}")
    return Command(
        op=_OP_BY_CODE[code],
        key=data[start:middle].decode("utf-8"),
        value=data[middle:end].decode("utf-8"),
    )


class KvStore:
    """A key-value map, and a count of how many commands built it."""

    __slots__ = ("_data", "applied")

    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        self.applied = 0

    @classmethod
    def replay(cls, records: Iterable[bytes]) -> "KvStore":
        """Rebuild from a log. This is what a node does when it comes back."""
        store = cls()
        for record in records:
            store.apply(decode_command(record))
        return store

    def apply(self, command: Command) -> None:
        if command.op == PUT:
            self._data[command.key] = command.value
        else:
            self._data.pop(command.key, None)
        self.applied += 1

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def all_keys(self) -> list[str]:
        """Sorted, always. Never iterate the underlying dict for an ordering decision."""
        return sorted(self._data)

    def snapshot(self) -> dict[str, str]:
        """A copy in sorted key order, so two equal stores serialise identically."""
        return {key: self._data[key] for key in sorted(self._data)}

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"KvStore({self.snapshot()}, applied={self.applied})"
