from __future__ import annotations
import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Basic default arguments; allow user to pass extra args after '--'
DEFAULT_ARGS = [
    "-q",  # quiet summary
    "--maxfail=1",
    "--disable-warnings",
]
# Coverage only if pytest-cov installed; we attempt gracefully
EXTRA_ARGS = []
try:
    import pytest_cov  # type: ignore  # noqa: F401
    EXTRA_ARGS.extend([
        "--cov=scripts",
        "--cov-report=term-missing:skip-covered",
    ])
except Exception:  # pragma: no cover
    pass

# Interpret custom args
user_args: list[str] = []
if "--" in sys.argv:
    idx = sys.argv.index("--")
    user_args = sys.argv[idx + 1 :]

cmd = [sys.executable, "-m", "pytest", "unit_tests"] + DEFAULT_ARGS + EXTRA_ARGS + user_args
print("Running:", " ".join(cmd))
res = subprocess.run(cmd, cwd=ROOT)
sys.exit(res.returncode)
