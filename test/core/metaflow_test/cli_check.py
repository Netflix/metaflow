import json
import os
import sys
import subprocess
from tempfile import NamedTemporaryFile

from metaflow.util import is_stringish
from . import MetaflowCheck, AssertArtifactFailed, AssertLogFailed, assert_equals, assert_exception, truncate
try:
    # Python 2
    import cPickle as pickle
except:
    # Python 3
    import pickle

class CliCheck(MetaflowCheck):
    def __init__(self, flow):
        from metaflow.client import Flow, get_namespace
        self.flow = flow
        self.run = Flow(flow.name)[self.run_id]
        assert_equals(sorted(step.name for step in flow),
                      sorted(step.id for step in self.run))
        self._test_namespace()

    def _test_namespace(self):
        from metaflow.client import Flow,\
                                    get_namespace,\
                                    namespace,\
                                    default_namespace
        from metaflow.exception import MetaflowNamespaceMismatch
        import os
        # test 1) USER should be the default
        assert_equals('user:%s' % os.environ.get('USER'),
                      get_namespace())
        # test 2) Run should be in the listing
        assert_equals(True,
                      self.run_id in [run.id for run in Flow(self.flow.name)])
        # test 3) changing namespace should change namespace
        namespace('user:nobody')
        assert_equals(get_namespace(), 'user:nobody')
        # test 4) fetching results in the incorrect namespace should fail
        assert_exception(lambda: Flow(self.flow.name)[self.run_id],
                         MetaflowNamespaceMismatch)
        # test 5) global namespace should work
        namespace(None)
        assert_equals(get_namespace(), None)
        Flow(self.flow.name)[self.run_id]
        default_namespace()

    def get_run(self):
        return self.run

    def run_cli(self, args, capture_output=False):
        cmd = [sys.executable, 'test_flow.py']
        cmd.extend(self.cli_options)
        cmd.extend(args)
        if capture_output:
            return subprocess.check_output(cmd)
        else:
            subprocess.check_call(cmd)

    def assert_artifact(self, step, name, value, fields=None):
        for task, artifacts in self.artifact_dict(step, name).items():
            if name in artifacts:
                artifact = artifacts[name]
                if fields:
                    for field, v in fields.items():
                        if is_stringish(artifact):
                            data = json.loads(artifact)
                        else:
                            data = artifact
                        if not isinstance(data, dict):
                            raise AssertArtifactFailed(
                                "Task '%s' expected %s to be a dictionary (got %s)" %
                                (task, name, type(data)))
                        if data.get(field, None) != v:
                            raise AssertArtifactFailed(
                                "Task '%s' expected %s[%s]=%r but got %s[%s]=%s" %
                                (task, name, field, truncate(value), name, field,
                                    truncate(data[field])))
                elif artifact != value:
                    raise AssertArtifactFailed(
                        "Task '%s' expected %s=%r but got %s=%s" %
                        (task, name, truncate(value), name, truncate(artifact)))
            else:
                raise AssertArtifactFailed("Task '%s' expected %s=%s but "
                                           "the key was not found" %\
                                            (task, name, truncate(value)))
        return True

    def artifact_dict(self, step, name):
        with NamedTemporaryFile(dir='.') as tmp:
            cmd = ['dump',
                   '--max-value-size', '100000000000',
                   '--private',
                   '--include', name,
                   '--file', tmp.name,
                   '%s/%s' % (self.run_id, step)]
            self.run_cli(cmd)
            with open(tmp.name, 'rb') as f:
                # if the step had multiple tasks, this will fail
                return pickle.load(f)

    def assert_log(self, step, logtype, value, exact_match=True):
        cmd = ['--quiet',
               'logs',
               '--%s' % logtype,
               '%s/%s' % (self.run_id, step)]
        log = self.run_cli(cmd, capture_output=True).decode('utf-8')
        if (exact_match and log != value) or\
           (not exact_match and value not in log):

            raise AssertLogFailed(
                "Task '%s/%s' expected %s log '%s' but got '%s'" %\
                (self.run_id,
                 step,
                 logtype,
                 repr(value),
                 repr(log)))
        return True
