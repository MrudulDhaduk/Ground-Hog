"""Command line interface.

Kept separate from `__main__` so tests can drive it without tripping the re-exec
guard. Subcommands (`run`, `sweep`, `replay`) arrive in M7; for now this exists so
that M0 has a program you can actually execute.
"""

import argparse
from collections.abc import Sequence

from groundhog import __version__

PROG = "groundhog"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="A Raft KV store and its deterministic simulation harness.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"{PROG} {__version__}",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    parser.parse_args(argv)
    parser.print_help()
    return 0
