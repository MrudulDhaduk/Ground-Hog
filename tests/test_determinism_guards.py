"""Static enforcement of the determinism rules.

`seed in -> run out` only holds if nothing in the package can reach outside the
simulation for a number, a timestamp, or an ordering. Code review does not catch
this reliably; a scanner does.

The scanner parses every module in `groundhog/` and rejects a fixed list of
constructs. When a milestone genuinely needs one of them -- the sweep runner needs
`multiprocessing`, the real-world adapters in M9 need `socket` and `time` -- the file
is added to `ALLOWLIST` with the reason, and stays visible forever after.
"""

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Final

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
PACKAGE_ROOT: Final = REPO_ROOT / "groundhog"

BANNED_MODULES: Final[dict[str, str]] = {
    "asyncio": "rule 5: no event loop but ours",
    "threading": "rule 6: one run is one thread",
    "multiprocessing": "rule 6: parallelism only across seeds, never inside one",
    "concurrent": "rule 6: parallelism only across seeds, never inside one",
    "subprocess": "rule 6: a simulation run does not spawn processes",
    "socket": "the network is simulated; a real socket is a real, unordered network",
    "select": "implies real I/O waiting",
    "time": "rule 4: time is a Tick from the Clock, not the wall clock",
    "datetime": "rule 4: time is int microseconds",
    "random": "rule 1: all randomness goes through sim.rng.Rng",
    "secrets": "rule 1: all randomness goes through sim.rng.Rng",
    "uuid": "uuid4 is unseeded randomness; ids come from the simulator",
    "tempfile": "generates unseeded random path names",
}

BANNED_ATTRIBUTES: Final[dict[tuple[str, str], str]] = {
    ("os", "urandom"): "rule 1: unseeded randomness",
    ("os", "getpid"): "varies per run and leaks into ordering",
    ("os", "times"): "wall clock",
}

BANNED_CALLS: Final[dict[str, str]] = {
    "id": "rule 5: memory addresses differ between runs; never order by them",
    "set": "rule 3: set iteration order is a landmine. Use dict[K, None]",
    "frozenset": "rule 3: set iteration order is a landmine. Use dict[K, None]",
}

#: path relative to the repo root -> the banned names that file is allowed to use.
ALLOWLIST: Final[dict[str, frozenset[str]]] = {
    # The PYTHONHASHSEED guard restarts the interpreter before any simulation exists.
    "groundhog/__main__.py": frozenset({"subprocess"}),
    # The one place `random` is allowed: it wraps a single seeded Random and every
    # other module draws from that. This is rule 1's single point of entry.
    "groundhog/sim/rng.py": frozenset({"random"}),
}


@dataclass(frozen=True)
class Violation:
    path: str
    lineno: int
    name: str
    reason: str

    def __str__(self) -> str:
        return f"{self.path}:{self.lineno}: {self.name!r} is banned -- {self.reason}"


def _root_module(dotted: str) -> str:
    return dotted.split(".")[0]


def scan_source(source: str, path: str) -> list[Violation]:
    """Return every banned construct in `source`, ignoring the allowlist."""
    found: list[Violation] = []

    def report(node: ast.AST, name: str, reason: str) -> None:
        lineno = getattr(node, "lineno", 0)
        found.append(Violation(path=path, lineno=lineno, name=name, reason=reason))

    for node in ast.walk(ast.parse(source, filename=path)):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = _root_module(alias.name)
                if root in BANNED_MODULES:
                    report(node, root, BANNED_MODULES[root])

        elif isinstance(node, ast.ImportFrom):
            # level > 0 is a relative import: `from .sim import rng`.
            if node.level == 0 and node.module is not None:
                root = _root_module(node.module)
                if root in BANNED_MODULES:
                    report(node, root, BANNED_MODULES[root])

        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):
                key = (node.value.id, node.attr)
                if key in BANNED_ATTRIBUTES:
                    report(node, f"{key[0]}.{key[1]}", BANNED_ATTRIBUTES[key])

        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in BANNED_CALLS:
                report(node, node.func.id, BANNED_CALLS[node.func.id])

        elif isinstance(node, ast.Set | ast.SetComp):
            report(node, "set", BANNED_CALLS["set"])

        elif isinstance(node, ast.AsyncFunctionDef | ast.Await | ast.AsyncFor | ast.AsyncWith):
            report(node, "async", BANNED_MODULES["asyncio"])

        elif isinstance(node, ast.FunctionDef) and node.name == "__del__":
            report(node, "__del__", "rule 5: finalizer order is not deterministic")

    return found


def scan_package() -> list[Violation]:
    """Scan every module under `groundhog/`, minus whatever the allowlist forgives."""
    violations: list[Violation] = []
    for source_file in sorted(PACKAGE_ROOT.rglob("*.py")):
        relative = source_file.relative_to(REPO_ROOT).as_posix()
        permitted = ALLOWLIST.get(relative, frozenset())
        violations.extend(
            v
            for v in scan_source(source_file.read_text(encoding="utf-8"), relative)
            if v.name not in permitted
        )
    return violations


def test_no_forbidden_imports() -> None:
    violations = scan_package()
    assert not violations, "determinism rules violated:\n" + "\n".join(str(v) for v in violations)


def test_package_is_not_empty() -> None:
    # A scanner that finds nothing because it looked nowhere proves nothing.
    assert list(PACKAGE_ROOT.rglob("*.py"))


def test_allowlist_has_no_stale_entries() -> None:
    missing = [path for path in ALLOWLIST if not (REPO_ROOT / path).exists()]
    assert not missing, f"ALLOWLIST names files that no longer exist: {missing}"


def test_allowlist_entries_are_actually_used() -> None:
    """An exemption nobody needs is an exemption nobody notices going stale."""
    unnecessary: list[str] = []
    for path, permitted in ALLOWLIST.items():
        source = (REPO_ROOT / path).read_text(encoding="utf-8")
        used = {v.name for v in scan_source(source, path)}
        unnecessary.extend(f"{path}: {name}" for name in sorted(permitted) if name not in used)
    assert not unnecessary, f"ALLOWLIST grants unused exemptions: {unnecessary}"


def test_scanner_detects_each_banned_construct() -> None:
    """The scanner is a test, so it needs a test that proves it fires."""
    cases: dict[str, str] = {
        "random": "import random",
        "time": "from time import monotonic",
        "threading": "import threading, sys",
        "os.urandom": "import os\nos.urandom(8)",
        "id": "x = id(object())",
        "set": "seen = set()",
        "async": "async def f() -> None:\n    pass",
        "__del__": "class C:\n    def __del__(self) -> None:\n        pass",
    }
    for expected, source in cases.items():
        names = {v.name for v in scan_source(source, "<case>")}
        assert expected in names, f"scanner missed {expected!r} in {source!r}"

    # And it stays quiet on code that obeys the rules.
    clean = "import os\nfrom groundhog.types import Tick\n\nseen: dict[int, None] = {}\n"
    assert not scan_source(clean, "<clean>")
