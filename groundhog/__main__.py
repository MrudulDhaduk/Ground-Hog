"""Process entry point, and the guard that makes hashing deterministic.

Determinism rule 2. CPython randomises `hash(str)` per process unless
`PYTHONHASHSEED` is set. That randomisation changes **set and dict-key iteration
order between runs**, which is precisely the thing that destroys replay: the same
seed would take a different path through the simulation and the trace would not
match.

The environment variable is read by the interpreter at startup, long before any of
our code runs, so it cannot be fixed from inside the process. The only remedy is to
notice it is wrong and start over -- hence the re-exec below.
"""

import os
import subprocess
import sys
from collections.abc import Sequence

from groundhog import cli

REQUIRED_HASH_SEED = "0"

#: Set on the child so a broken environment cannot fork-bomb us.
_REEXEC_MARKER = "GROUNDHOG_REEXECED"


def ensure_deterministic_hashing() -> None:
    """Re-exec this process with `PYTHONHASHSEED=0` if it is not already set.

    Returns normally when the environment is already correct. Otherwise it runs the
    identical command in a child process and exits with the child's status, so the
    caller never sees this function return.

    Never call this from inside pytest: it would re-exec the test runner.
    """
    if os.environ.get("PYTHONHASHSEED") == REQUIRED_HASH_SEED:
        return

    if os.environ.get(_REEXEC_MARKER) == "1":
        # We already restarted once and it still is not set. Something is stripping
        # the environment; refuse to loop.
        raise RuntimeError(
            "PYTHONHASHSEED is still not 0 after a re-exec. Set it manually: "
            f"PYTHONHASHSEED={REQUIRED_HASH_SEED}"
        )

    env = dict(os.environ)
    env["PYTHONHASHSEED"] = REQUIRED_HASH_SEED
    env[_REEXEC_MARKER] = "1"

    # `sys.orig_argv` is the interpreter's own argv, so interpreter flags (-X, -O,
    # `-m groundhog`) survive the restart. `sys.argv` would lose them.
    argv = list(sys.orig_argv[1:]) if sys.orig_argv else ["-m", "groundhog", *sys.argv[1:]]

    completed = subprocess.run([sys.executable, *argv], env=env, check=False)
    raise SystemExit(completed.returncode)


def main(argv: Sequence[str] | None = None) -> int:
    ensure_deterministic_hashing()
    return cli.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
