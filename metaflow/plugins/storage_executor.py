import math
import multiprocessing
import os
import platform
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from metaflow.exception import MetaflowException

if sys.version_info[:2] < (3, 7):
    # in 3.6, Only BrokenProcessPool exists (there is no BrokenThreadPool)
    from concurrent.futures.process import BrokenProcessPool

    BrokenStorageExecutorError = BrokenProcessPool
else:
    # in 3.7 and newer, BrokenExecutor is a base class that parents BrokenProcessPool AND BrokenThreadPool
    from concurrent.futures import BrokenExecutor as _BrokenExecutor

    BrokenStorageExecutorError = _BrokenExecutor


def _determine_effective_cpu_limit():
    """Calculate CPU limit (in number of cores) based on:

    - /sys/fs/cgroup/cpu/cpu.max (if available, cgroup 2)
    OR
    - /sys/fs/cgroup/cpu/cpu.cfs_quota_us
    - /sys/fs/cgroup/cpu/cpu.cfs_period_us

    Returns:
        > 0 if limit was successfully calculated
        = 0 if we determined that there is no limit
        -1 if we failed to determine the limit
    """
    try:
        if platform.system() == "Darwin":
            # On MacOS, and not in a container
            # We are assuming it is extremely out-of-the-way to run a Darwin container
            return 0
        elif platform.system() == "Linux":
            # Bare metal Linux, or a Linux container running on either Linux or MacOS system
            # Linux containers on MacOS have this cpu.max file
            with open("/sys/fs/cgroup/cpu.max", "rb") as f:
                parts = f.read().decode("utf-8").split(" ")
                if len(parts) == 2:
                    if parts[0] == "max":
                        return 0
                    return int(parts[0]) / int(parts[1])

            with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "rb") as f:
                quota = int(f.read())
                # this file shows -1 for no limit
                if quota == -1:
                    return 0
                # some other negative number - this is weird...return "undetermined"
                if quota < 0:
                    return -1
            with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "rb") as f:
                period = int(f.read())
                # should be positive. we don't want div by zero errors.
                if period <= 0:
                    return -1
                return quota / period
        else:
            # Not Linux or MacOS...give up and return "undetermined"
            return -1
    except Exception:
        return -1


def _noop_for_executor_warm_up():
    pass


def _compute_executor_max_workers():
    # For processes, let's start conservative. Let's restrict to 4-18 always. Can be configurable one day.
    min_processes = 4
    max_processes = 18
    # make an effort to get cgroup cpu limits
    effective_cpu_limit = _determine_effective_cpu_limit()

    def _bracket(min_v, v, max_v):
        assert min_v <= max_v
        if v < min_v:
            return min_v
        if v > max_v:
            return max_v
        return v

    if effective_cpu_limit < 0:
        # We don't know the limit, stick to min
        processpool_max_workers = min_processes
    elif effective_cpu_limit == 0:
        # There is (probably) no limit, use physical core count
        processpool_max_workers = _bracket(
            min_processes, os.cpu_count() or 1, max_processes
        )
    else:
        # There is a limit, so let's bracket it within min / max
        processpool_max_workers = _bracket(
            min_processes, math.ceil(effective_cpu_limit), max_processes
        )
    # Threads a lighter than processes... so just tack on a little more
    threadpool_max_workers = processpool_max_workers + 4
    return processpool_max_workers, threadpool_max_workers


# TODO keyboard interrupt crazy tracebacks in process pool
class StorageExecutor(object):
    """Thin wrapper around a ProcessPoolExecutor, or a ThreadPoolExecutor where
    the former may be unsafe.
    """

    def __init__(self, use_processes=False):
        (
            processpool_max_workers,
            threadpool_max_workers,
        ) = _compute_executor_max_workers()
        if use_processes:
            mp_start_method = multiprocessing.get_start_method(allow_none=True)
            if mp_start_method == "spawn":
                self._executor = ProcessPoolExecutor(
                    max_workers=processpool_max_workers
                )
            elif sys.version_info[:2] >= (3, 7):
                self._executor = ProcessPoolExecutor(
                    mp_context=multiprocessing.get_context("spawn"),
                    max_workers=processpool_max_workers,
                )
            else:
                raise MetaflowException(
                    msg="Cannot use ProcessPoolExecutor because Python version is older than 3.7 and multiprocess start method has been set to something other than 'spawn'"
                )
        else:
            self._executor = ThreadPoolExecutor(max_workers=threadpool_max_workers)

    def warm_up(self):
        # warm up at least one process or thread in the pool.
        # we don't await future... just let it complete in background
        self._executor.submit(_noop_for_executor_warm_up)

    def submit(self, *args, **kwargs):
        return self._executor.submit(*args, **kwargs)


def handle_executor_exceptions(func):
    """
    Decorator for handling errors that come from an Executor. This decorator should
    only be used on functions where executor errors are possible. I.e. the function
    uses StorageExecutor.
    """

    def inner_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BrokenStorageExecutorError:
            # This is fatal. So we bail ASAP.
            # We also don't want to log, because KeyboardInterrupt on worker processes
            # also take us here, so it's going to be "normal" user operation most of the
            # time.
            # BrokenExecutor parents both BrokenThreadPool and BrokenProcessPool.
            sys.exit(1)

    return inner_function
