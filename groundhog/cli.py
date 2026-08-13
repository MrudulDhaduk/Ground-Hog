"""Command line interface.

Kept separate from `__main__` so tests can drive it without tripping the re-exec
guard. `run`, `sweep` and `replay` arrive in M7; `demo` is M1's proof of life.
"""

import argparse
from collections.abc import Callable, Sequence

from groundhog import __version__
from groundhog.sim.demo import run_demo
from groundhog.sim.trace import open_trace
from groundhog.types import MILLISECOND

PROG = "groundhog"
DEFAULT_SEED = 4471


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="A Raft KV store and its deterministic simulation harness.",
    )
    parser.add_argument("--version", action="version", version=f"{PROG} {__version__}")

    subcommands = parser.add_subparsers(title="commands", metavar="COMMAND")

    demo = subcommands.add_parser(
        "demo",
        help="run the two-node ping-pong world (M1 proof of life)",
        description="Two toy nodes exchanging messages over the deterministic core.",
    )
    demo.add_argument("--seed", type=int, default=DEFAULT_SEED, help="the universe to run")
    demo.add_argument(
        "--trace",
        metavar="PATH",
        default=None,
        help="write a JSONL trace here; '-' for stdout",
    )
    demo.add_argument(
        "--ms",
        type=int,
        default=1000,
        help="how much virtual time to simulate, in milliseconds (default: 1000)",
    )
    demo.set_defaults(handler=_cmd_demo)

    return parser


def _cmd_demo(args: argparse.Namespace) -> int:
    seed: int = args.seed
    max_ticks: int = args.ms * MILLISECOND
    with open_trace(args.trace) as trace:
        sim, result = run_demo(seed, trace=trace, max_ticks=max_ticks)

    print(
        f"seed {result.seed}  events {result.events}  "
        f"final_tick {result.final_tick}  rng_draws {result.rng_calls}  "
        f"stopped {result.stop_reason}"
    )
    for node_id in sim.node_ids():
        node = sim.nodes[node_id]
        print(
            f"  node {node.node_id}  sent {node.sent}  "
            f"received {node.received}  dropped {node.dropped}"
        )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handler: Callable[[argparse.Namespace], int] | None = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0
    return handler(args)
