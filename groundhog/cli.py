"""Command line interface.

Kept separate from `__main__` so tests can drive it without tripping the re-exec
guard. `run`, `sweep` and `replay` arrive in M7; `demo` is M1's proof of life.
"""

import argparse
from collections.abc import Callable, Sequence

from groundhog import __version__
from groundhog.sim.demo import DEFAULT_NODES, profile_by_name, run_demo
from groundhog.sim.faults import PROFILES
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
        help="run the toy cluster through the simulator",
        description="Toy nodes chattering over the simulated network, disk and clock.",
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
    demo.add_argument(
        "--nodes",
        type=int,
        default=DEFAULT_NODES,
        help=f"how many nodes in the cluster (default: {DEFAULT_NODES})",
    )
    demo.add_argument(
        "--faults",
        choices=sorted(PROFILES),
        default="quiet",
        help="how badly the world behaves (default: quiet)",
    )
    demo.set_defaults(handler=_cmd_demo)

    return parser


def _cmd_demo(args: argparse.Namespace) -> int:
    profile = profile_by_name(args.faults)
    with open_trace(args.trace) as trace:
        cluster, result = run_demo(
            args.seed,
            trace=trace,
            node_count=args.nodes,
            profile=profile,
            max_ticks=args.ms * MILLISECOND,
        )

    schedule = cluster.schedule
    print(
        f"seed {result.seed}  faults {profile.name}  events {result.events}  "
        f"final_tick {result.final_tick}  rng_draws {result.rng_calls}  "
        f"stopped {result.stop_reason}"
    )
    print(f"  network   delivered {cluster.network.delivered}  dropped {cluster.network.dropped}")
    print(
        f"  scheduled {len(schedule.partitions)} partitions, {len(schedule.outages)} node outages"
    )
    for node_id in cluster.sim.node_ids():
        node = cluster.sim.nodes[node_id]
        print(
            f"  node {node_id}   pongs {node.pongs}  durable {len(node.storage.durable_records())}"
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
