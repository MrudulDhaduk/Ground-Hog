"""Write-ahead log framing: `[u32 length][u32 crc32][payload]`, big-endian.

Hand-rolled on purpose. `pickle` is not version-stable and would hide the framing, and
JSON cannot express a write that stopped halfway through a byte. The whole point of
rung 2 is that a log is a byte stream that a crash can cut anywhere, so the format has
to be something a crash can cut.

**The checksum covers the length field as well as the payload.** If it covered only the
payload, a corrupted length would be read as gospel: the reader would skip the wrong
number of bytes and land in the middle of the next record, where it would find some
other record's checksum and possibly agree with it. Four bytes of extra coverage removes
that whole class of confusion.

Recovery rule: read records until one does not check out, and stop. Everything before
that point is the log; everything after is gone. A log is only ever a valid **prefix**
of what was written -- never a valid log with a hole in it.
"""

import struct
from dataclasses import dataclass
from typing import Final
from zlib import crc32

_HEADER = struct.Struct(">II")
_LENGTH = struct.Struct(">I")

HEADER_SIZE: Final = _HEADER.size

#: A sanity bound. Nothing this project writes comes close; a length above it means the
#: bytes are garbage, and allocating on garbage is how a corrupt file becomes a crash.
MAX_RECORD_SIZE: Final = 1 << 26  # 64 MiB


@dataclass(frozen=True, slots=True)
class Scan:
    """The result of reading a log image that may have been cut short."""

    records: list[bytes]
    #: Byte offset at which each record starts. Same length as `records`.
    offsets: list[int]
    #: Length of the decodable prefix. Bytes past this are torn or corrupt.
    valid_bytes: int
    #: How many bytes recovery would throw away.
    discarded_bytes: int

    @property
    def is_torn(self) -> bool:
        return self.discarded_bytes > 0


def encode_record(payload: bytes) -> bytes:
    if len(payload) > MAX_RECORD_SIZE:
        raise ValueError(f"record of {len(payload)} bytes exceeds {MAX_RECORD_SIZE}")
    checksum = crc32(_LENGTH.pack(len(payload)) + payload)
    return _HEADER.pack(len(payload), checksum) + payload


def encode_records(payloads: list[bytes]) -> bytes:
    return b"".join(encode_record(payload) for payload in payloads)


def decode_records(data: bytes) -> Scan:
    """Read as many whole, checksummed records as the image actually contains.

    Never raises. A torn or corrupt tail is a normal thing to find after a crash, not an
    error -- the caller's job is to discard it, which is what `valid_bytes` is for.
    """
    records: list[bytes] = []
    offsets: list[int] = []
    offset = 0
    size = len(data)

    while offset + HEADER_SIZE <= size:
        length, checksum = _HEADER.unpack_from(data, offset)
        if length > MAX_RECORD_SIZE:
            break
        end = offset + HEADER_SIZE + length
        if end > size:
            break  # the payload was cut short: a torn write
        payload = data[offset + HEADER_SIZE : end]
        if crc32(data[offset : offset + _LENGTH.size] + payload) != checksum:
            break  # the bytes are there but they are not what was written
        records.append(payload)
        offsets.append(offset)
        offset = end

    return Scan(
        records=records,
        offsets=offsets,
        valid_bytes=offset,
        discarded_bytes=size - offset,
    )
