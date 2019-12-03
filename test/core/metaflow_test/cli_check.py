import os
import sys
import subprocess
from tempfile import NamedTemporaryFile

from . import MetaflowCheck, AssertArtifactFailed, AssertLogFailed, truncate

try:
    # Python 2
    import cPickle as pickle
except:
    # Python 3
    import pickle

class CliCheck(MetaflowCheck):

    def run_cli(self, args, capture_output=False):
        cmd = [sys.executable, 'test_flow.py']
        cmd.extend(self.cli_options)
        cmd.extend(args)
        if capture_output:
            return subprocess.check_output(cmd)
        else:
            subprocess.check_call(cmd)

    def assert_artifact(self, step, name, value):
        for task, artifacts in self.artifact_dict(step, name).items():
            if name in artifacts:
                if artifacts[name] != value:
                    raise AssertArtifactFailed("Task '%s' expected %s=%s "
                                               "but got %s=%s" %\
                                               (task,
                                                name,
                                                truncate(value),
                                                name,
                                                truncate(artifacts[name])))
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
