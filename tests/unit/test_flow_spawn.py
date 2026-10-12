"""Regression test for https://github.com/avito-tech/aqueduct/issues/66

Flow(..., mp_start_method='spawn') failed to start because the parent process
dropped its last reference to each step's Worker (and the mp.RLock objects inside it)
right after Process.start(). The SemLock finalizer unlinked the named semaphores
before the spawned child had unpickled them, so the child died with
FileNotFoundError in SemLock._rebuild and the start barrier timed out.

The 'spawn' start method is process-global, while the rest of the unit tests rely on
'fork' (see tests/unit/conftest.py), so the flow is started in a dedicated subprocess.
"""
import asyncio
import multiprocessing as mp
import pathlib
import subprocess
import sys

from aqueduct.flow import Flow, FlowStep
from aqueduct.handler import BaseTaskHandler

STARTED_MARKER = 'FLOW_STARTED_WITH_SPAWN'


class NoOpHandlerFirst(BaseTaskHandler):
    def handle(self, *tasks):
        pass


class NoOpHandlerSecond(BaseTaskHandler):
    def handle(self, *tasks):
        pass


def run_spawn_flow():
    """Entry point executed in a separate interpreter."""
    mp.set_start_method('spawn')

    async def main():
        flow = Flow(
            FlowStep(handler=NoOpHandlerFirst()),
            FlowStep(handler=NoOpHandlerSecond()),
            mp_start_method='spawn',
        )
        flow.start(timeout=30)
        print(STARTED_MARKER, flush=True)
        await flow.stop(graceful=False)

    asyncio.get_event_loop().run_until_complete(main())


def test_flow_starts_with_spawn_start_method():
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, '-c', f'from {__name__} import run_spawn_flow; run_spawn_flow()'],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert STARTED_MARKER in result.stdout, result.stderr
    assert 'FileNotFoundError' not in result.stderr, result.stderr
