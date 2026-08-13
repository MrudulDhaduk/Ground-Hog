"""The M1 done-when, as executable claims.

    "the same seed produces a byte-identical trace file across two runs, two
    processes, and a reboot. If this is ever false, stop everything and fix it --
    every later milestone is worthless without it."

Two runs and two processes are testable here. A reboot is not, but the thing a reboot
would perturb is per-process hash randomisation, and the cross-process test below runs
with `PYTHONHASHSEED` deliberately stripped from the environment -- so the guard is
carrying that case, and `test_cli.py` proves the guard works.
"""

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from groundhog.sim.demo import run_demo
from groundhog.sim.trace import open_trace
from groundhog.types import MILLISECOND, JsonValue

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
SEED: Final = 4471
DEMO_MS: Final = 500
MAX_TICKS: Final = DEMO_MS * MILLISECOND


def as_int(value: JsonValue) -> int:
    assert isinstance(value, int)
    return value


class ListTrace:
    """An in-memory `Trace`. Structural typing, so no registration and no base class."""

    def __init__(self) -> None:
        self.records: list[Mapping[str, JsonValue]] = []

    def write(self, record: Mapping[str, JsonValue]) -> None:
        self.records.append(record)


def trace_to_file(seed: int, path: Path) -> None:
    with open_trace(str(path)) as trace:
        run_demo(seed, trace=trace, max_ticks=MAX_TICKS)


def trace_in_subprocess(seed: int, path: Path) -> None:
    env = dict(os.environ)
    env.pop("PYTHONHASHSEED", None)
    env.pop("GROUNDHOG_REEXECED", None)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "groundhog",
            "demo",
            "--seed",
            str(seed),
            "--ms",
            str(DEMO_MS),
            "--trace",
            str(path),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def records(path: Path) -> list[dict[str, JsonValue]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def test_the_same_seed_replays_byte_identically(tmp_path: Path) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    trace_to_file(SEED, first)
    trace_to_file(SEED, second)
    assert first.read_bytes() == second.read_bytes()


def test_the_same_seed_replays_byte_identically_in_a_fresh_process(tmp_path: Path) -> None:
    first = tmp_path / "proc1.jsonl"
    second = tmp_path / "proc2.jsonl"
    trace_in_subprocess(SEED, first)
    trace_in_subprocess(SEED, second)
    assert first.read_bytes() == second.read_bytes()


def test_in_process_and_out_of_process_runs_agree(tmp_path: Path) -> None:
    """The strongest form: the library and the CLI are the same universe."""
    library = tmp_path / "library.jsonl"
    command = tmp_path / "command.jsonl"
    trace_to_file(SEED, library)
    trace_in_subprocess(SEED, command)
    assert library.read_bytes() == command.read_bytes()


def test_a_different_seed_is_a_different_universe(tmp_path: Path) -> None:
    """Otherwise byte-identity would be satisfied by writing nothing at all."""
    a = tmp_path / "a.jsonl"
    b = tmp_path / "b.jsonl"
    trace_to_file(SEED, a)
    trace_to_file(SEED + 1, b)
    assert a.read_bytes() != b.read_bytes()


def test_the_trace_is_worth_comparing(tmp_path: Path) -> None:
    path = tmp_path / "trace.jsonl"
    trace_to_file(SEED, path)
    assert len(records(path)) > 100


def test_the_trace_uses_unix_newlines(tmp_path: Path) -> None:
    """Windows text mode would silently turn every \\n into \\r\\n."""
    path = tmp_path / "trace.jsonl"
    trace_to_file(SEED, path)
    assert b"\r\n" not in path.read_bytes()


def test_events_appear_in_tick_then_seq_order(tmp_path: Path) -> None:
    path = tmp_path / "trace.jsonl"
    trace_to_file(SEED, path)
    keys = [
        (as_int(record["tick"]), as_int(record["seq"]))
        for record in records(path)
        if "seq" in record and not str(record["kind"]).startswith("sim.")
    ]
    assert keys == sorted(keys)
    assert len(keys) == len(dict.fromkeys(keys))


def test_the_rng_counter_only_moves_forward(tmp_path: Path) -> None:
    path = tmp_path / "trace.jsonl"
    trace_to_file(SEED, path)
    draws = [as_int(record["rng"]) for record in records(path) if "rng" in record]
    assert draws == sorted(draws)
    assert draws[-1] > 0


def test_the_result_summary_is_reproducible() -> None:
    first = run_demo(SEED, trace=ListTrace(), max_ticks=MAX_TICKS)[1]
    second = run_demo(SEED, trace=ListTrace(), max_ticks=MAX_TICKS)[1]
    assert first == second
    assert first.events > 60


def test_node_state_is_reproducible() -> None:
    def counters() -> list[tuple[int, int, int]]:
        cluster, _ = run_demo(SEED, trace=ListTrace(), max_ticks=MAX_TICKS)
        return [
            (
                node_id,
                cluster.sim.nodes[node_id].pongs,
                len(cluster.sim.nodes[node_id].storage.image()),
            )
            for node_id in cluster.sim.node_ids()
        ]

    assert counters() == counters()


def test_the_demo_exercises_every_piece_of_the_core() -> None:
    trace = ListTrace()
    run_demo(SEED, trace=trace, max_ticks=MAX_TICKS)
    kinds = dict.fromkeys(str(record["kind"]) for record in trace.records)
    for expected in ("sim.start", "ping_timer", "net.deliver", "sim.end"):
        assert expected in kinds, f"{expected} never happened; kinds seen: {sorted(kinds)}"
