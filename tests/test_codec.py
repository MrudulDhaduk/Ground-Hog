"""Framing and recovery. A crash can cut the bytes anywhere; the reader must cope."""

import pytest

from groundhog.codec import (
    HEADER_SIZE,
    MAX_RECORD_SIZE,
    decode_records,
    encode_record,
    encode_records,
)

PAYLOADS = [b"alpha", b"", b"charlie" * 5, b"d", bytes(range(256))]


def test_header_is_eight_bytes() -> None:
    assert HEADER_SIZE == 8
    assert len(encode_record(b"")) == 8
    assert len(encode_record(b"abc")) == 11


def test_a_record_round_trips() -> None:
    scan = decode_records(encode_record(b"hello"))
    assert scan.records == [b"hello"]
    assert not scan.is_torn


def test_many_records_round_trip_in_order() -> None:
    scan = decode_records(encode_records(PAYLOADS))
    assert scan.records == PAYLOADS
    assert scan.discarded_bytes == 0


def test_offsets_point_at_the_start_of_each_record() -> None:
    blob = encode_records(PAYLOADS)
    scan = decode_records(blob)
    for offset, payload in zip(scan.offsets, PAYLOADS, strict=True):
        assert decode_records(blob[offset:]).records[0] == payload
    assert scan.valid_bytes == len(blob)


def test_an_empty_image_decodes_to_nothing() -> None:
    scan = decode_records(b"")
    assert scan.records == []
    assert scan.valid_bytes == 0
    assert not scan.is_torn


def test_cutting_at_any_byte_offset_leaves_a_valid_prefix() -> None:
    """The whole property, at the level where it is actually decided."""
    blob = encode_records(PAYLOADS)
    for cut in range(len(blob) + 1):
        scan = decode_records(blob[:cut])
        assert scan.records == PAYLOADS[: len(scan.records)], f"cut at {cut}"
        assert scan.valid_bytes <= cut
        assert scan.discarded_bytes == cut - scan.valid_bytes


def test_every_prefix_length_is_reachable() -> None:
    """Cutting somewhere must actually lose records, or the test above proves nothing."""
    blob = encode_records(PAYLOADS)
    counts = {len(decode_records(blob[:cut]).records) for cut in range(len(blob) + 1)}
    assert counts == set(range(len(PAYLOADS) + 1))


def test_corrupting_any_single_byte_stops_the_scan_at_or_before_it() -> None:
    blob = bytearray(encode_records(PAYLOADS))
    for index in range(len(blob)):
        flipped = bytearray(blob)
        flipped[index] ^= 0xFF
        scan = decode_records(bytes(flipped))
        assert scan.valid_bytes <= index, f"byte {index} was accepted"
        assert scan.records == PAYLOADS[: len(scan.records)]


def test_a_corrupted_length_field_is_caught() -> None:
    """This is the reason the checksum covers the length and not just the payload."""
    blob = bytearray(encode_records([b"first", b"second"]))
    blob[3] = 0x40  # claim a much longer first record
    scan = decode_records(bytes(blob))
    assert scan.records == []
    assert scan.is_torn


def test_an_absurd_length_does_not_get_believed() -> None:
    blob = bytearray(encode_record(b"x"))
    blob[0:4] = (MAX_RECORD_SIZE + 1).to_bytes(4, "big")
    assert decode_records(bytes(blob)).records == []


def test_a_torn_tail_does_not_hide_the_good_records_before_it() -> None:
    good = encode_records([b"keep", b"these"])
    scan = decode_records(good + encode_record(b"lost")[:-2])
    assert scan.records == [b"keep", b"these"]
    assert scan.valid_bytes == len(good)
    assert scan.discarded_bytes == HEADER_SIZE + 4 - 2


def test_encoding_refuses_an_oversized_record() -> None:
    with pytest.raises(ValueError, match="exceeds"):
        encode_record(b"x" * (MAX_RECORD_SIZE + 1))


def test_identical_payloads_encode_identically() -> None:
    """No timestamps, no nonces, nothing that would make two runs differ."""
    assert encode_record(b"same") == encode_record(b"same")
