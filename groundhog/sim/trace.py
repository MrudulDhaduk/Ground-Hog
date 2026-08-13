"""JSONL traces: one line per thing that happened, replayable and diffable.

The trace is the evidence. Two runs of the same seed must produce byte-identical
files, so every choice here is about removing wobble:

- `sort_keys=True` -- a record built by a different code path with the same fields
  still serialises identically.
- `separators` without spaces -- no incidental whitespace.
- `ensure_ascii=True` -- no dependency on the reader's encoding.
- `allow_nan=False` -- floats are already excluded by `JsonValue`; this makes a stray
  one an error rather than the token `NaN`.
- `newline="\\n"` on the file -- Windows would otherwise translate to CRLF, and a trace
  captured on one platform would not diff against the same trace captured on another.
"""

import json
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Protocol, TextIO

from groundhog.types import JsonValue

#: `--trace -` writes to stdout instead of a file.
STDOUT_SPEC = "-"


class Trace(Protocol):
    """Somewhere to put a record. Implementations must not reorder or drop."""

    def write(self, record: Mapping[str, JsonValue]) -> None: ...


class NullTrace:
    """Discards everything. The default, so tracing is never the reason a run is slow."""

    __slots__ = ()

    def write(self, record: Mapping[str, JsonValue]) -> None:
        return None


class JsonlTrace:
    """One compact JSON object per line."""

    __slots__ = ("_stream", "lines")

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream
        self.lines = 0

    def write(self, record: Mapping[str, JsonValue]) -> None:
        self._stream.write(
            json.dumps(
                record,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
        self._stream.write("\n")
        self.lines += 1


@contextmanager
def open_trace(spec: str | None) -> Iterator[Trace]:
    """`None` -> discard, `-` -> stdout, anything else -> that path."""
    if spec is None:
        yield NullTrace()
    elif spec == STDOUT_SPEC:
        yield JsonlTrace(sys.stdout)
    else:
        with Path(spec).open("w", encoding="utf-8", newline="\n") as stream:
            yield JsonlTrace(stream)
