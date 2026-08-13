"""The checker that decides whether rung 3 failed.

A checker that has never caught anything is not known to work, so this file spends most
of its time constructing divergences by hand and proving the checker sees them.
"""

import pytest

from groundhog.invariants.divergence import DivergenceChecker
from groundhog.kv import Command, KvStore
from groundhog.naive.client import LostWrite, expected_state, lost_writes
from groundhog.types import NodeId


def store(**pairs: str) -> KvStore:
    built = KvStore()
    for key, value in pairs.items():
        built.apply(Command.put(key, value))
    return built


def three(a: KvStore, b: KvStore, c: KvStore) -> dict[NodeId, KvStore]:
    return {1: a, 2: b, 3: c}


def test_identical_replicas_agree() -> None:
    checker = DivergenceChecker(three(store(x="1"), store(x="1"), store(x="1")))
    assert checker.check() == []


def test_empty_replicas_agree() -> None:
    assert DivergenceChecker(three(store(), store(), store())).check() == []


def test_a_different_value_is_caught() -> None:
    checker = DivergenceChecker(three(store(x="1"), store(x="2"), store(x="1")))
    divergences = checker.check()
    assert len(divergences) == 1
    assert divergences[0].key == "x"
    assert divergences[0].values == ((1, "1"), (2, "2"), (3, "1"))


def test_a_missing_key_is_caught() -> None:
    """A backup that never got the write is not "eventually consistent", it is wrong."""
    divergences = DivergenceChecker(three(store(x="1"), store(), store(x="1"))).check()
    assert len(divergences) == 1
    assert divergences[0].values == ((1, "1"), (2, None), (3, "1"))


def test_an_extra_key_is_caught() -> None:
    """The nastier direction: a replica holding something nobody else has."""
    divergences = DivergenceChecker(three(store(), store(ghost="boo"), store())).check()
    assert [d.key for d in divergences] == ["ghost"]


def test_every_divergent_key_is_reported() -> None:
    checker = DivergenceChecker(three(store(a="1", b="1"), store(a="2", b="2"), store()))
    assert [d.key for d in checker.check()] == ["a", "b"]


def test_keys_come_out_sorted() -> None:
    checker = DivergenceChecker(three(store(z="1", a="1"), store(), store()))
    assert [d.key for d in checker.check()] == ["a", "z"]


def test_comparing_needs_something_to_compare_with() -> None:
    with pytest.raises(ValueError, match="fewer than two"):
        DivergenceChecker({1: store()})


def test_a_divergence_describes_itself_usefully() -> None:
    divergences = DivergenceChecker(three(store(x="1"), store(x="2"), store())).check()
    assert divergences[0].describe() == "key 'x': n1='1', n2='2', n3=None"


# -- what the client was promised -----------------------------------------------------


def test_the_last_acked_write_per_key_is_what_counts() -> None:
    acked = [Command.put("x", "1"), Command.put("y", "9"), Command.put("x", "2")]
    assert expected_state(acked) == {"x": "2", "y": "9"}


def test_an_acked_delete_means_the_key_must_be_absent() -> None:
    acked = [Command.put("x", "1"), Command.delete("x")]
    assert expected_state(acked) == {"x": None}


def test_a_replica_holding_the_promised_value_is_not_a_loss() -> None:
    acked = [Command.put("x", "1")]
    assert lost_writes(acked, three(store(x="1"), store(x="1"), store(x="1"))) == []


def test_a_replica_missing_a_promised_value_is_a_loss() -> None:
    acked = [Command.put("x", "1")]
    lost = lost_writes(acked, three(store(x="1"), store(), store(x="1")))
    assert lost == [LostWrite(node=2, key="x", promised="1", found=None)]


def test_everyone_agreeing_on_the_wrong_value_is_still_a_loss() -> None:
    """Strictly stronger than divergence, and the reason both checks exist. Three
    replicas can agree perfectly on a value the client was never promised."""
    acked = [Command.put("x", "2")]
    stores = three(store(x="1"), store(x="1"), store(x="1"))

    assert DivergenceChecker(stores).check() == []
    assert len(lost_writes(acked, stores)) == 3


def test_an_unacked_write_is_not_owed_to_anyone() -> None:
    """A write that got no answer may or may not have landed. Holding the system to
    those would be inventing requirements it never took on."""
    assert lost_writes([], three(store(x="1"), store(), store())) == []
