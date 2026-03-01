"""
Unit tests for get_recent_run_ids / write_latest_run_id in metaflow/util.py.
"""

import os
import sys
import tempfile
import types
import unittest
import shutil


def _load_util(tmpdir):
    _METAFLOW_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
    sys.path.insert(0, _METAFLOW_ROOT)

    fake_ls_mod = types.ModuleType("metaflow.plugins.datastores.local_storage")

    class FakeLocalStorage:
        datastore_root = tmpdir

        @classmethod
        def get_datastore_root_from_config(cls, echo, create_on_absent=True):
            return tmpdir

        @staticmethod
        def path_join(*args):
            return os.path.join(*args)

    fake_ls_mod.LocalStorage = FakeLocalStorage
    sys.modules["metaflow.plugins.datastores.local_storage"] = fake_ls_mod

    for mod_name in (
        "metaflow",
        "metaflow._vendor",
        "metaflow._vendor.packaging",
        "metaflow._vendor.packaging.version",
    ):
        if mod_name not in sys.modules:
            sys.modules[mod_name] = types.ModuleType(mod_name)
    sys.modules["metaflow._vendor.packaging.version"].parse = lambda x: x

    import importlib.util as ilu

    spec = ilu.spec_from_file_location(
        "metaflow.util",
        os.path.join(_METAFLOW_ROOT, "metaflow", "util.py"),
    )
    util_mod = ilu.module_from_spec(spec)
    sys.modules["metaflow.util"] = util_mod
    spec.loader.exec_module(util_mod)
    return util_mod


def _noop_echo(msg, **kw):
    pass


class _FlowObj:
    def __init__(self, flow_name="TestFlow"):
        class _Flow:
            name = flow_name

        self.flow = _Flow()
        self.echo = _noop_echo


class TestRecentRunIds(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.util = _load_util(self.tmpdir)
        self.obj = _FlowObj()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write(self, *run_ids):
        for rid in run_ids:
            self.util.write_latest_run_id(self.obj, rid)

    def _history(self):
        return self.util.get_recent_run_ids(_noop_echo, "TestFlow")

    def test_empty_when_no_runs(self):
        self.assertEqual(self.util.get_recent_run_ids(_noop_echo, "NoSuchFlow"), [])

    def test_newest_first_ordering(self):
        self._write("10", "20", "30")
        self.assertEqual([e["run_id"] for e in self._history()], ["30", "20", "10"])

    def test_duplicate_run_id_deduplicated(self):
        self._write("1", "2", "3")
        self._write("2")  # re-resume the same run
        ids = [e["run_id"] for e in self._history()]
        self.assertEqual(ids.count("2"), 1)
        self.assertEqual(ids[0], "2")

    def test_history_capped_at_max_recent_runs(self):
        cap = self.util.MAX_RECENT_RUNS
        self._write(*[str(i) for i in range(cap + 5)])
        self.assertEqual(len(self._history()), cap)


if __name__ == "__main__":
    unittest.main()
