"""The state machine. Boring on purpose, and it must stay boring."""

import pytest

from groundhog.kv import Command, KvStore, decode_command, encode_command


def test_put_and_get() -> None:
    store = KvStore()
    store.apply(Command.put("x", "5"))
    assert store.get("x") == "5"


def test_a_missing_key_is_none_not_an_error() -> None:
    assert KvStore().get("nope") is None


def test_the_last_write_wins() -> None:
    store = KvStore()
    store.apply(Command.put("x", "1"))
    store.apply(Command.put("x", "2"))
    assert store.get("x") == "2"


def test_delete_removes_a_key() -> None:
    store = KvStore()
    store.apply(Command.put("x", "5"))
    store.apply(Command.delete("x"))
    assert store.get("x") is None
    assert len(store) == 0


def test_deleting_a_missing_key_is_fine() -> None:
    """Applying an entry must never fail. A state machine that can reject an entry is
    one two replicas can disagree about, which puts a consensus problem in the one
    place that must not have one."""
    store = KvStore()
    store.apply(Command.delete("never existed"))
    assert len(store) == 0


def test_applied_counts_every_command_including_no_ops() -> None:
    store = KvStore()
    store.apply(Command.delete("ghost"))
    store.apply(Command.put("x", "1"))
    assert store.applied == 2


def test_snapshot_is_sorted_so_equal_stores_serialise_identically() -> None:
    forwards = KvStore()
    backwards = KvStore()
    for key in ("a", "b", "c"):
        forwards.apply(Command.put(key, key.upper()))
    for key in ("c", "b", "a"):
        backwards.apply(Command.put(key, key.upper()))

    assert forwards.snapshot() == backwards.snapshot()
    assert list(forwards.snapshot()) == list(backwards.snapshot()) == ["a", "b", "c"]


def test_all_keys_is_sorted() -> None:
    store = KvStore()
    for key in ("zebra", "apple", "mango"):
        store.apply(Command.put(key, "x"))
    assert store.all_keys() == ["apple", "mango", "zebra"]


def test_order_changes_the_answer() -> None:
    """The whole reason replication order matters. Two replicas applying the same two
    commands in different orders hold different data, and neither is corrupt."""
    first = KvStore()
    first.apply(Command.put("x", "1"))
    first.apply(Command.put("x", "2"))

    second = KvStore()
    second.apply(Command.put("x", "2"))
    second.apply(Command.put("x", "1"))

    assert first.snapshot() != second.snapshot()


# -- encoding -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        Command.put("x", "5"),
        Command.put("", ""),
        Command.delete("x"),
        Command.put("key with spaces", "value\nwith\nnewlines"),
        Command.put("unicode éè", "中文"),
        Command.put("big", "v" * 10_000),
    ],
)
def test_commands_round_trip(command: Command) -> None:
    assert decode_command(encode_command(command)) == command


def test_encoding_is_stable() -> None:
    """No timestamps, no nonces: the same command is the same bytes, every run."""
    assert encode_command(Command.put("x", "5")) == encode_command(Command.put("x", "5"))


def test_an_unknown_operation_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown operation"):
        encode_command(Command(op="increment", key="x"))


def test_a_truncated_command_is_refused() -> None:
    blob = encode_command(Command.put("x", "5"))
    with pytest.raises(ValueError, match="header claims"):
        decode_command(blob[:-1])


def test_a_log_can_be_replayed_into_a_store() -> None:
    """What a node does when it comes back from the dead."""
    commands = [Command.put("a", "1"), Command.put("b", "2"), Command.delete("a")]
    store = KvStore.replay(encode_command(command) for command in commands)
    assert store.snapshot() == {"b": "2"}


def test_replaying_a_prefix_gives_the_state_at_that_point() -> None:
    """Which is exactly what a torn WAL tail leaves behind."""
    commands = [Command.put("a", "1"), Command.put("a", "2"), Command.put("a", "3")]
    encoded = [encode_command(command) for command in commands]
    assert KvStore.replay(encoded[:2]).get("a") == "2"
