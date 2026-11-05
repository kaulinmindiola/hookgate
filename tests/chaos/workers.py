"""A real worker in its own OS process, for the chaos suite."""

import subprocess
import sys
from pathlib import Path
from typing import Protocol

from tests.chaos.services import REPO_ROOT, wait_until

WORKER_SCRIPT = Path(__file__).with_name("chaos_worker.py")


class WorkerProcess:
    """A real worker in its own OS process; ``kill()`` gives it no chance to clean up."""

    def __init__(self, signal_dir: Path, log_path: Path, env: dict[str, str]) -> None:
        self.signal_dir = signal_dir
        self.log_path = log_path
        self._log = log_path.open("wb")
        self.process = subprocess.Popen(
            [sys.executable, str(WORKER_SCRIPT)],
            cwd=REPO_ROOT,
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        wait_until(
            lambda: (signal_dir / "ready").exists() or self.process.poll() is not None,
            timeout_s=60,
            what="the worker to start consuming",
        )
        if self.process.poll() is not None:
            raise RuntimeError(f"worker exited at start-up:\n{self.log()}")

    def claimed(self, event_id: str) -> bool:
        return (self.signal_dir / f"{event_id}.claimed").exists()

    def kill(self) -> None:
        """SIGKILL on Linux and macOS, TerminateProcess on Windows: no cleanup at all."""
        self.process.kill()
        self.process.wait(timeout=30)

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.kill()
        self._log.close()

    def log(self) -> str:
        return self.log_path.read_text(errors="replace")


class StartWorker(Protocol):
    def __call__(self, *, block_prefix: str = "", lease_s: int = 2) -> WorkerProcess: ...
