import os
import tempfile
import shutil

import pytest

from metaflow.multicore_utils import (
    parallel_map,
    parallel_imap_unordered,
    MulticoreException,
)


def test_parallel_map():
    assert parallel_map(lambda s: s.upper(), ["a", "b", "c", "d", "e", "f"]) == [
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
    ]


def _double(x):
    return x * 2


def _fail_on_three(x):
    if x == 3:
        raise ValueError("intentional failure")
    return x * 2


def _leaked_temp_files(directory):
    return [f for f in os.listdir(directory) if f.startswith("parallel_map_")]


def test_parallel_map_cleans_temp_files_on_success():
    tmpdir = tempfile.mkdtemp()
    try:
        result = parallel_map(_double, [1, 2, 3, 4], max_parallel=2, dir=tmpdir)
        assert sorted(result) == [2, 4, 6, 8]
        assert _leaked_temp_files(tmpdir) == []
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_parallel_imap_unordered_cleans_temp_files_on_child_failure():
    tmpdir = tempfile.mkdtemp()
    try:
        with pytest.raises(MulticoreException):
            list(
                parallel_imap_unordered(
                    _fail_on_three, range(5), max_parallel=1, dir=tmpdir
                )
            )
        assert (
            _leaked_temp_files(tmpdir) == []
        ), "Temp files leaked after child failure: %s" % _leaked_temp_files(tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_parallel_map_cleans_temp_files_on_child_failure():
    tmpdir = tempfile.mkdtemp()
    try:
        with pytest.raises(MulticoreException):
            parallel_map(_fail_on_three, [1, 2, 3, 4], max_parallel=2, dir=tmpdir)
        assert (
            _leaked_temp_files(tmpdir) == []
        ), "Temp files leaked after child failure: %s" % _leaked_temp_files(tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
