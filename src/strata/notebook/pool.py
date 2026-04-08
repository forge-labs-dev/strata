"""Warm process pool for fast cell execution.

The pool pre-spawns Python processes with common imports already loaded,
reducing the startup overhead from ~1.5s to ~50ms per execution.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio.subprocess

logger = logging.getLogger(__name__)


@dataclass
class WarmProcess:
    """A pre-spawned Python process ready for work."""

    process: asyncio.subprocess.Process
    created_at: float
    ready: bool = False


class WarmProcessPool:
    """Pool of pre-spawned Python processes for fast cell execution.

    The pool maintains a configured number of Python processes that are kept warm
    with common imports already loaded. When a cell needs to execute, we acquire
    a warm process, send the execution manifest, and wait for results.

    Attributes:
        notebook_dir: Path to the notebook directory
        pool_size: Number of warm processes to maintain
    """

    def __init__(
        self,
        notebook_dir: Path,
        pool_size: int = 2,
        python_executable: str | Path = "python",
    ):
        """Initialize the warm process pool.

        Args:
            notebook_dir: Path to the notebook directory
            pool_size: Number of warm processes to maintain
            python_executable: Python interpreter used for warm workers
        """
        self.notebook_dir = Path(notebook_dir)
        self.pool_size = pool_size
        self.python_executable = str(python_executable)
        self._available: asyncio.Queue[WarmProcess] = asyncio.Queue()
        self._warming: int = 0  # Processes currently starting up
        self._started: bool = False
        self._lock = asyncio.Lock()
        # Track background spawn tasks so drain() can cancel them
        self._background_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Spawn initial pool of warm processes.

        Spawns pool_size processes in parallel.
        """
        async with self._lock:
            if self._started:
                return
            self._started = True

        # Spawn processes in parallel
        tasks = [self._spawn_warm_process() for _ in range(self.pool_size)]
        await asyncio.gather(*tasks, return_exceptions=True)

    def track_background_task(self, task: asyncio.Task) -> None:
        """Track a task so shutdown paths can cancel it."""
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _spawn_warm_process(self) -> None:
        """Spawn a process that imports common deps and waits for work.

        Uses a pool_worker.py script that:
        1. Imports common packages
        2. Sends a 'ready' signal
        3. Waits for a manifest path on stdin
        4. Runs the harness logic
        5. Exits (one-shot)
        """
        self._warming += 1
        try:
            worker_script = Path(__file__).parent / "pool_worker.py"

            # Spawn the pool worker process
            process = await asyncio.create_subprocess_exec(
                self.python_executable,
                str(worker_script),
                str(self.notebook_dir),
                stdout=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(self.notebook_dir),
            )

            # Wait for the 'ready' signal
            try:
                assert process.stdout is not None
                ready_line = await asyncio.wait_for(process.stdout.readline(), timeout=10.0)
                if ready_line and b"ready" in ready_line.lower():
                    warm_proc = WarmProcess(
                        process=process,
                        created_at=time.time(),
                        ready=True,
                    )
                    await self._available.put(warm_proc)
                    logger.debug(f"Warm process spawned and ready (pid={process.pid})")
                else:
                    logger.warning("Warm process did not send ready signal, killing")
                    process.kill()
                    try:
                        await asyncio.wait_for(process.wait(), timeout=2.0)
                    except TimeoutError:
                        pass
            except TimeoutError:
                logger.warning("Warm process startup timed out, killing process")
                process.kill()
                try:
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                except TimeoutError:
                    pass

        except Exception as e:
            logger.error(f"Failed to spawn warm process: {e}")
        finally:
            self._warming -= 1

    async def acquire(self) -> WarmProcess | None:
        """Get a warm process. If none available, return None (caller uses cold spawn).

        Returns:
            WarmProcess if available, None if pool is empty or not started
        """
        try:
            warm_proc = self._available.get_nowait()
            return warm_proc
        except asyncio.QueueEmpty:
            return None

    async def release_and_replace(self, process: WarmProcess) -> None:
        """Kill used process and spawn a replacement in background.

        Args:
            process: The WarmProcess to kill
        """
        # Kill the used process
        if process.process and process.process.returncode is None:
            process.process.kill()
            try:
                await asyncio.wait_for(process.process.wait(), timeout=2.0)
            except TimeoutError:
                logger.warning("Warm process kill timeout")

        # Spawn a replacement in background (tracked so drain() can cancel it)
        task = asyncio.create_task(self._spawn_warm_process())
        self.track_background_task(task)

    async def drain(self) -> None:
        """Kill all processes in the pool (on env change or shutdown).

        Cancels pending background spawn tasks, then drains and kills
        all queued processes.
        """
        async with self._lock:
            self._started = False

        # Cancel any in-flight background spawn tasks first so they don't
        # put new processes into the queue after we've drained it.
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        # Drain all processes from queue
        while True:
            try:
                proc = self._available.get_nowait()
                if proc.process and proc.process.returncode is None:
                    proc.process.kill()
                    try:
                        await asyncio.wait_for(proc.process.wait(), timeout=2.0)
                    except TimeoutError:
                        pass
            except asyncio.QueueEmpty:
                break

    async def invalidate(self) -> None:
        """Environment changed — drain and respawn.

        Called when uv.lock has changed, indicating dependencies changed.
        """
        logger.info("Invalidating warm process pool due to env change")
        await self.drain()
        await self.start()

    def shutdown_nowait(self) -> None:
        """Best-effort synchronous shutdown for non-async callers."""
        self._started = False
        for task in list(self._background_tasks):
            task.cancel()
        self._background_tasks.clear()

        while True:
            try:
                proc = self._available.get_nowait()
            except asyncio.QueueEmpty:
                break
            if proc.process and proc.process.returncode is None:
                try:
                    proc.process.kill()
                except ProcessLookupError:
                    pass


class PooledCellExecutor:
    """Wrapper that uses the warm process pool for execution.

    This is a helper class for CellExecutor to use the pool when available.
    """

    @staticmethod
    async def execute_with_pool(
        pool: WarmProcessPool,
        manifest_path: Path,
        notebook_dir: Path,
        timeout_seconds: float = 30,
    ) -> dict | None:
        """Execute a cell using a warm process from the pool.

        Args:
            pool: The WarmProcessPool instance
            manifest_path: Path to the execution manifest
            notebook_dir: Path to the notebook directory
            timeout_seconds: Execution timeout

        Returns:
            Result dict if successful, None if pool not available (caller should use cold)
        """
        # Try to acquire a warm process
        warm_proc = await pool.acquire()
        if warm_proc is None:
            return None

        try:
            # Send manifest path to the warm process
            assert warm_proc.process.stdin is not None
            assert warm_proc.process.stdout is not None
            manifest_str = (str(manifest_path) + "\n").encode()
            warm_proc.process.stdin.write(manifest_str)
            await warm_proc.process.stdin.drain()

            # Wait for result
            result_json = await asyncio.wait_for(
                warm_proc.process.stdout.readline(),
                timeout=timeout_seconds,
            )

            if result_json:
                result_data = json.loads(result_json.decode())
                return result_data
            else:
                logger.warning("Warm process returned empty result")
                return None

        except asyncio.CancelledError:
            logger.info(
                "Warm process execution cancelled; killing worker pid=%s",
                warm_proc.process.pid,
            )
            if warm_proc.process.returncode is None:
                warm_proc.process.kill()
                try:
                    await asyncio.shield(warm_proc.process.wait())
                except Exception:
                    logger.exception(
                        "Failed waiting for cancelled warm worker pid=%s",
                        warm_proc.process.pid,
                    )
            raise
        except TimeoutError:
            logger.warning("Warm process execution timed out")
            return None
        except Exception as e:
            logger.error(f"Error executing with warm process: {e}")
            return None
        finally:
            # Kill used process and spawn replacement
            await asyncio.shield(pool.release_and_replace(warm_proc))
