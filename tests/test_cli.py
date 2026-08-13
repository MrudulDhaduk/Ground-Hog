"""The program runs, and it runs with hashing pinned.

The re-exec shim is the kind of code that silently stops working -- it only matters
when the environment is wrong, which is never the case in a shell you already fixed.
So these tests deliberately run with `PYTHONHASHSEED` stripped out.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Final

import pytest

from groundhog import __version__, cli

REPO_ROOT: Final = Path(__file__).resolve().parent.parent


def run_python(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Run a child interpreter with a hostile (unpinned) environment."""
    env = dict(os.environ)
    env.pop("PYTHONHASHSEED", None)
    env.pop("GROUNDHOG_REEXECED", None)
    return subprocess.run(
        [sys.executable, *args],
        capture_output=True,
        text=True,
        cwd=cwd or REPO_ROOT,
        env=env,
        check=False,
    )


def test_version_flag_prints_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"groundhog {__version__}"


def test_bare_invocation_prints_help() -> None:
    assert cli.main([]) == 0


def test_module_entry_point_prints_version() -> None:
    result = run_python("-m", "groundhog", "--version")
    assert result.returncode == 0, result.stderr
    assert f"groundhog {__version__}" in result.stdout


def test_environment_really_does_randomize_hashing() -> None:
    """Control case. If this fails, the guard below is testing nothing."""
    result = run_python("-c", "import sys; print(sys.flags.hash_randomization)")
    assert result.stdout.strip() == "1", result.stderr


def test_guard_reexecs_with_hashing_pinned(tmp_path: Path) -> None:
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import os, sys\n"
        "from groundhog.__main__ import ensure_deterministic_hashing\n"
        "ensure_deterministic_hashing()\n"
        "print(os.environ.get('PYTHONHASHSEED'), sys.flags.hash_randomization)\n",
        encoding="utf-8",
    )
    result = run_python(str(probe))
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0 0", result.stdout


def test_guard_returns_immediately_when_already_pinned(tmp_path: Path) -> None:
    """No re-exec when the environment is already correct: one process, not two."""
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import os\n"
        "from groundhog.__main__ import ensure_deterministic_hashing\n"
        "os.environ['GROUNDHOG_REEXECED'] = 'poisoned'\n"
        "ensure_deterministic_hashing()\n"
        "print('returned')\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    result = subprocess.run(
        [sys.executable, str(probe)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "returned"
