"""Command line interface.

Kept separate from `__main__` so tests can drive it without tripping the re-exec
guard. `run`, `sweep` and `replay` arrive in M7; `demo` is M1's proof of life.
"""

import argparse
from collections.abc import Callable, Sequence

from groundhog import __version__
from groundhog.naive.replicator import IMPLEMENTED as REPLICATOR_IMPLEMENTED
from groundhog.naive.world import DEFAULT_KEYS, DEFAULT_WRITES, run_naive
from groundhog.raft.node import IMPLEMENTED as RAFT_IMPLEMENTED
from groundhog.raft.world import MUTATIONS, run_raft
from groundhog.sim.demo import DEFAULT_NODES, run_demo
from groundhog.sim.faults import PROFILES, FaultProfile, profile_by_name
from groundhog.sim.trace import NullTrace, open_trace
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

    naive = subcommands.add_parser(
        "naive",
        help="run the rung-3 replicator and check whether the copies agree",
        description=(
            "One primary, two backups, fire-and-forget replication. This is supposed "
            "to break; the exercise is finding out how little it takes."
        ),
    )
    naive.add_argument("--seed", type=int, default=DEFAULT_SEED, help="the universe to run")
    naive.add_argument(
        "--scan",
        metavar="FROM:TO",
        default=None,
        help="run a range of seeds and print the ones that break (e.g. 0:2000)",
    )
    naive.add_argument(
        "--faults",
        choices=sorted(PROFILES),
        default="quiet",
        help="how badly the world behaves (default: quiet)",
    )
    naive.add_argument("--writes", type=int, default=DEFAULT_WRITES, help="client writes to issue")
    naive.add_argument("--keys", type=int, default=DEFAULT_KEYS, help="size of the key space")
    naive.add_argument(
        "--trace",
        metavar="PATH",
        default=None,
        help="write a JSONL trace here; '-' for stdout",
    )
    naive.set_defaults(handler=_cmd_naive)

    raft = subcommands.add_parser(
        "raft",
        help="run a 3-node Raft cluster under the simulator",
        description="Elect a leader, replicate writes, survive the leader dying.",
    )
    raft.add_argument("--seed", type=int, default=DEFAULT_SEED, help="the universe to run")
    raft.add_argument(
        "--faults",
        choices=sorted(PROFILES),
        default="quiet",
        help="how badly the world behaves (default: quiet)",
    )
    raft.add_argument("--writes", type=int, default=20, help="client writes to issue")
    raft.add_argument(
        "--trace",
        metavar="PATH",
        default=None,
        help="write a JSONL trace here; '-' for stdout",
    )
    raft.add_argument(
        "--scan",
        metavar="FROM:TO",
        default=None,
        help="run a range of seeds and print the ones that violate an invariant",
    )
    raft.add_argument(
        "--mutate",
        choices=MUTATIONS,
        default="none",
        help="deliberately break a rule, to prove the checkers notice (default: none)",
    )
    raft.add_argument(
        "--no-check",
        action="store_true",
        help="run without the invariant checkers",
    )
    raft.set_defaults(handler=_cmd_raft)

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


def _cmd_naive(args: argparse.Namespace) -> int:
    if not REPLICATOR_IMPLEMENTED:
        print(
            "naive/replicator.py is not written yet.\n"
            "That file is yours (M4 [Y]). Read its module docstring, fill in the four\n"
            "methods, then set IMPLEMENTED = True."
        )
        return 2

    profile = profile_by_name(args.faults)
    if args.scan is not None:
        return _scan_seeds(args, profile.name)

    with open_trace(args.trace) as trace:
        result = run_naive(
            args.seed,
            trace=trace,
            profile=profile,
            writes=args.writes,
            keys=args.keys,
        )

    print(result.summary())
    if not result.quiescent:
        print("  WARNING: the run did not settle; this verdict is not trustworthy")
    for node_id in sorted(result.stores):
        print(f"  node {node_id}  {result.stores[node_id]}")
    for divergence in result.divergences:
        print(f"  DIVERGED  {divergence.describe()}")
    for lost in result.lost:
        print(f"  LOST      {lost.describe()}")
    return 0 if result.agrees and result.kept_its_promises else 1


def _scan_seeds(args: argparse.Namespace, profile_name: str) -> int:
    """A sequential seed scan, so rung 3 is doable before M7 builds the real sweep."""
    start, _, stop = args.scan.partition(":")
    profile = profile_by_name(args.faults)
    broken = 0
    checked = 0

    for seed in range(int(start), int(stop)):
        result = run_naive(
            seed,
            trace=NullTrace(),
            profile=profile,
            writes=args.writes,
            keys=args.keys,
        )
        checked += 1
        if not (result.agrees and result.kept_its_promises):
            broken += 1
            print(result.summary())

    print(f"\n{broken} of {checked} seeds broke under '{profile_name}'")
    return 1 if broken else 0


def _cmd_raft(args: argparse.Namespace) -> int:
    if not RAFT_IMPLEMENTED:
        print(
            "raft/node.py is not finished yet.\n"
            "The six consensus functions are yours (M5 [C->Y]). Read raft/figure2.md\n"
            "beside the paper, fill them in, then set IMPLEMENTED = True."
        )
        return 2

    profile = profile_by_name(args.faults)
    if args.scan is not None:
        return _scan_raft_seeds(args, profile)

    with open_trace(args.trace) as trace:
        result = run_raft(
            args.seed,
            trace=trace,
            profile=profile,
            writes=args.writes,
            check=not args.no_check,
            mutate=args.mutate,
        )

    print(result.summary())
    for violation in result.violations:
        print(f"  {violation.describe()}")
        print(f"  replay: {violation.reproduce(faults=args.faults)}")
    if not result.violations and not result.quiescent:
        print("  WARNING: the run did not settle")
    for node_id in sorted(result.stores):
        print(f"  node {node_id}  commit {result.committed[node_id]}  {result.stores[node_id]}")
    return 1 if result.violations else 0


def _scan_raft_seeds(args: argparse.Namespace, profile: FaultProfile) -> int:
    """A sequential seed scan. M7 replaces this with a parallel sweep and a shrinker."""
    start, _, stop = args.scan.partition(":")
    broken = 0
    checked = 0

    for seed in range(int(start), int(stop)):
        result = run_raft(
            seed,
            trace=NullTrace(),
            profile=profile,
            writes=args.writes,
            check=not args.no_check,
            mutate=args.mutate,
        )
        checked += 1
        if result.violations:
            broken += 1
            violation = result.violations[0]
            print(violation.describe())
            print(f"  replay: {violation.reproduce(faults=args.faults)}")

    print(f"\n{broken} of {checked} seeds violated an invariant under '{profile.name}'")
    if args.mutate != "none":
        print(f"(mutation active: {args.mutate})")
    return 1 if broken else 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handler: Callable[[argparse.Namespace], int] | None = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0
    return handler(args)
