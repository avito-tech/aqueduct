"""
Copied from torch.multiprocessing.spawn v1.7.0
Added:
- inherits logger directly `logger = log.getChild('multiprocessing')`
- logs errors (e.g. for sending to Sentry)
"""

import multiprocessing
import multiprocessing.connection
import signal
import sys
from typing import List, Optional

from .exceptions import AqueductError
from .logger import log

logger = log.getChild('multiprocessing')


class ProcessException(AqueductError):
    __slots__ = ['error_index', 'error_pid']

    def __init__(self, msg: str, error_index: int, pid: int):
        super().__init__(msg)
        self.error_index = error_index
        self.pid = pid


class ProcessRaisedException(ProcessException):
    """
    Exception is thrown when the process failed due to exception
    raised by the code.
    """
    def __init__(
        self,
        msg: str,
        error_index: int,
        error_pid: int,
    ):
        super().__init__(msg, error_index, error_pid)


class ProcessExitedException(ProcessException):
    """
    Exception is thrown when the process failed due to signal
    or exited with a specific code.
    """
    __slots__ = ['exit_code']

    def __init__(
            self, msg: str, error_index: int, error_pid: int,
            exit_code: int, signal_name: Optional[str] = None
    ):
        super().__init__(msg, error_index, error_pid)
        self.exit_code = exit_code
        self.signal_name = signal_name


def _wrap(fn, i, args, error_queue):
    try:
        fn(i, *args)
    except KeyboardInterrupt:
        pass  # SIGINT; Killed by parent, do nothing
    except Exception:
        # Propagate exception to parent process, keeping original traceback
        import traceback
        logger.exception(traceback.format_exc())
        error_queue.put(traceback.format_exc())
        sys.exit(1)


class ProcessContext:
    def __init__(self, processes: List[multiprocessing.process.BaseProcess],
                 error_queues: List[multiprocessing.SimpleQueue]):
        self.error_queues = error_queues
        self.processes = processes
        self.sentinels = {
            process.sentinel: index
            for index, process in enumerate(processes)
        }

    def __repr__(self):
        process = ','.join(map(str, self.processes))
        return f'<ProcessContext processes: {process}>'

    def pids(self):
        return [int(process.pid) for process in self.processes]

    def join(self, timeout=None):
        r"""
        Tries to join one or more processes in this process context.
        If one of them exited with a non-zero exit status, this function
        kills the remaining processes and raises an exception with the cause
        of the first process exiting.

        Returns ``True`` if all processes have been joined successfully,
        ``False`` if there are more processes that need to be joined.

        Arguments:
            timeout (float): Wait this long before giving up on waiting.
        """
        # Ensure this function can be called even when we're done.
        if len(self.sentinels) == 0:
            return True

        # Wait for any process to fail or all of them to succeed.
        ready = multiprocessing.connection.wait(
            self.sentinels.keys(),
            timeout=timeout,
        )

        error_index = None
        for sentinel in ready:
            index = self.sentinels.pop(sentinel)
            process = self.processes[index]
            process.join()
            if process.exitcode != 0:
                error_index = index
                break

        # Return if there was no error.
        if error_index is None:
            # Return whether or not all processes have been joined.
            return len(self.sentinels) == 0

        # Assume failure. Terminate processes that are still alive.
        for process in self.processes:
            if process.is_alive():
                process.terminate()
            process.join()

        # There won't be an error on the queue if the process crashed.
        failed_process = self.processes[error_index]
        if self.error_queues[error_index].empty():
            exitcode = self.processes[error_index].exitcode
            if exitcode < 0:
                name = signal.Signals(-exitcode).name
                raise ProcessExitedException(
                    'process %d terminated with signal %s' %
                    (error_index, name),
                    error_index=error_index,
                    error_pid=failed_process.pid,
                    exit_code=exitcode,
                    signal_name=name
                )
            else:
                raise ProcessExitedException(
                    'process %d terminated with exit code %d' %
                    (error_index, exitcode),
                    error_index=error_index,
                    error_pid=failed_process.pid,
                    exit_code=exitcode
                )

        original_trace = self.error_queues[error_index].get()
        msg = '\n\n-- Process %d terminated with the following error:\n' % error_index
        msg += original_trace
        raise ProcessRaisedException(msg, error_index, failed_process.pid)


def start_processes(fn, args=(), nprocs=1, join=True, daemon=False, start_method='spawn'):
    r"""Starts ``nprocs`` processes that run ``fn`` with ``args``.

    If one of the processes exits with a non-zero exit status, the
    remaining processes are killed and an exception is raised with the
    cause of termination. In the case an exception was caught in the
    child process, it is forwarded and its traceback is included in
    the exception raised in the parent process.

    Arguments:
        fn (function): Function is called as the entrypoint of the
            started process. This function must be defined at the top
            level of a module so it can be pickled and started. This
            is a requirement imposed by multiprocessing.

            The function is called as ``fn(i, *args)``, where ``i`` is
            the process index and ``args`` is the passed through tuple
            of arguments.

        args (tuple): Arguments passed to ``fn``.
        nprocs (int): Number of processes to start.
        join (bool): Perform a blocking join on all processes.
        daemon (bool): The started processes' daemon flag. If set to True,
                       daemonic processes will be created.
        start_method (string): Start method.

    Returns:
        None if ``join`` is ``True``,
        :class:`~ProcessContext` if ``join`` is ``False``

    """
    mp = multiprocessing.get_context(start_method)
    error_queues = []
    processes = []
    for i in range(nprocs):
        error_queue = mp.SimpleQueue()
        process = mp.Process(
            target=_wrap,
            args=(fn, i, args, error_queue),
            daemon=daemon,
        )
        process.start()
        error_queues.append(error_queue)
        processes.append(process)

    context = ProcessContext(processes, error_queues)
    if not join:
        return context

    # Loop on join until it returns True or raises an exception.
    while not context.join():
        pass
