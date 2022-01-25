# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import collections
import functools
from typing import Any, DefaultDict, Iterable, List, Tuple

from metaflow._vendor.pylint import reporters
from metaflow._vendor.pylint.lint.utils import _patch_sys_path
from metaflow._vendor.pylint.message import Message
from metaflow._vendor.pylint.typing import FileItem, MessageLocationTuple
from metaflow._vendor.pylint.utils import LinterStats, merge_stats

try:
    import multiprocessing
except ImportError:
    multiprocessing = None  # type: ignore[assignment]

# PyLinter object used by worker processes when checking files using multiprocessing
# should only be used by the worker processes
_worker_linter = None


def _get_new_args(message):
    location = (
        message.abspath,
        message.path,
        message.module,
        message.obj,
        message.line,
        message.column,
    )
    return (message.msg_id, message.symbol, location, message.msg, message.confidence)


def _worker_initialize(linter, arguments=None):
    global _worker_linter  # pylint: disable=global-statement
    _worker_linter = linter

    # On the worker process side the messages are just collected and passed back to
    # parent process as _worker_check_file function's return value
    _worker_linter.set_reporter(reporters.CollectingReporter())
    _worker_linter.open()

    # Patch sys.path so that each argument is importable just like in single job mode
    _patch_sys_path(arguments or ())


def _worker_check_single_file(
    file_item: FileItem,
) -> Tuple[
    int, Any, str, Any, List[Tuple[Any, ...]], LinterStats, Any, DefaultDict[Any, List]
]:
    if not _worker_linter:
        raise Exception("Worker linter not yet initialised")
    _worker_linter.open()
    _worker_linter.check_single_file_item(file_item)
    mapreduce_data = collections.defaultdict(list)
    for checker in _worker_linter.get_checkers():
        try:
            data = checker.get_map_data()
        except AttributeError:
            continue
        mapreduce_data[checker.name].append(data)
    msgs = [_get_new_args(m) for m in _worker_linter.reporter.messages]
    _worker_linter.reporter.reset()
    return (
        id(multiprocessing.current_process()),
        _worker_linter.current_name,
        file_item.filepath,
        _worker_linter.file_state.base_name,
        msgs,
        _worker_linter.stats,
        _worker_linter.msg_status,
        mapreduce_data,
    )


def _merge_mapreduce_data(linter, all_mapreduce_data):
    """Merges map/reduce data across workers, invoking relevant APIs on checkers"""
    # First collate the data, preparing it so we can send it to the checkers for
    # validation. The intent here is to collect all the mapreduce data for all checker-
    # runs across processes - that will then be passed to a static method on the
    # checkers to be reduced and further processed.
    collated_map_reduce_data = collections.defaultdict(list)
    for linter_data in all_mapreduce_data.values():
        for run_data in linter_data:
            for checker_name, data in run_data.items():
                collated_map_reduce_data[checker_name].extend(data)

    # Send the data to checkers that support/require consolidated data
    original_checkers = linter.get_checkers()
    for checker in original_checkers:
        if checker.name in collated_map_reduce_data:
            # Assume that if the check has returned map/reduce data that it has the
            # reducer function
            checker.reduce_map_data(linter, collated_map_reduce_data[checker.name])


def check_parallel(linter, jobs, files: Iterable[FileItem], arguments=None):
    """Use the given linter to lint the files with given amount of workers (jobs)
    This splits the work filestream-by-filestream. If you need to do work across
    multiple files, as in the similarity-checker, then inherit from MapReduceMixin and
    implement the map/reduce mixin functionality"""
    # The reporter does not need to be passed to worker processes, i.e. the reporter does
    original_reporter = linter.reporter
    linter.reporter = None

    # The linter is inherited by all the pool's workers, i.e. the linter
    # is identical to the linter object here. This is required so that
    # a custom PyLinter object can be used.
    initializer = functools.partial(_worker_initialize, arguments=arguments)
    pool = multiprocessing.Pool(  # pylint: disable=consider-using-with
        jobs, initializer=initializer, initargs=[linter]
    )
    # ..and now when the workers have inherited the linter, the actual reporter
    # can be set back here on the parent process so that results get stored into
    # correct reporter
    linter.set_reporter(original_reporter)
    linter.open()
    try:
        all_stats = []
        all_mapreduce_data = collections.defaultdict(list)

        # Maps each file to be worked on by a single _worker_check_single_file() call,
        # collecting any map/reduce data by checker module so that we can 'reduce' it
        # later.
        for (
            worker_idx,  # used to merge map/reduce data across workers
            module,
            file_path,
            base_name,
            messages,
            stats,
            msg_status,
            mapreduce_data,
        ) in pool.imap_unordered(_worker_check_single_file, files):
            linter.file_state.base_name = base_name
            linter.set_current_module(module, file_path)
            for msg in messages:
                msg = Message(
                    msg[0], msg[1], MessageLocationTuple(*msg[2]), msg[3], msg[4]
                )
                linter.reporter.handle_message(msg)  # type: ignore[attr-defined]  # linter.set_reporter() call above makes linter have a reporter attr
            all_stats.append(stats)
            all_mapreduce_data[worker_idx].append(mapreduce_data)
            linter.msg_status |= msg_status
    finally:
        pool.close()
        pool.join()

    _merge_mapreduce_data(linter, all_mapreduce_data)
    linter.stats = merge_stats([linter.stats] + all_stats)

    # Insert stats data to local checkers.
    for checker in linter.get_checkers():
        if checker is not linter:
            checker.stats = linter.stats
