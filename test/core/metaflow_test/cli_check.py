import os
import sys
import subprocess
import json
from tempfile import NamedTemporaryFile

from metaflow.util import is_stringish

from . import MetaflowCheck, AssertArtifactFailed, AssertLogFailed, truncate

try:
    # Python 2
    import cPickle as pickle
except:
    # Python 3
    import pickle


class CliCheck(MetaflowCheck):
    def run_cli(self, args, capture_output=False):
        cmd = [sys.executable, "test_flow.py"]

        # remove --quiet from top level options to capture output from echo
        # we will add --quiet in args if needed
        cmd.extend([opt for opt in self.cli_options if opt != "--quiet"])

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
                                "Task '%s' expected %s to be a dictionary (got %s)"
                                % (task, name, type(data))
                            )
                        if data.get(field, None) != v:
                            raise AssertArtifactFailed(
                                "Task '%s' expected %s[%s]=%r but got %s[%s]=%s"
                                % (
                                    task,
                                    name,
                                    field,
                                    truncate(value),
                                    name,
                                    field,
                                    truncate(data[field]),
                                )
                            )
                elif artifact != value:
                    raise AssertArtifactFailed(
                        "Task '%s' expected %s=%r but got %s=%s"
                        % (task, name, truncate(value), name, truncate(artifact))
                    )
            else:
                raise AssertArtifactFailed(
                    "Task '%s' expected %s=%s but "
                    "the key was not found" % (task, name, truncate(value))
                )
        return True

    def artifact_dict(self, step, name):
        with NamedTemporaryFile(dir=".") as tmp:
            cmd = [
                "dump",
                "--max-value-size",
                "100000000000",
                "--private",
                "--include",
                name,
                "--file",
                tmp.name,
                "%s/%s" % (self.run_id, step),
            ]
            self.run_cli(cmd)
            with open(tmp.name, "rb") as f:
                # if the step had multiple tasks, this will fail
                return pickle.load(f)

    def artifact_dict_if_exists(self, step, name):
        return self.artifact_dict(step, name)

    def assert_log(self, step, logtype, value, exact_match=True):
        log = self.get_log(step, logtype)
        if (exact_match and log != value) or (not exact_match and value not in log):

            raise AssertLogFailed(
                "Task '%s/%s' expected %s log '%s' but got '%s'"
                % (self.run_id, step, logtype, repr(value), repr(log))
            )
        return True

    def get_log(self, step, logtype):
        cmd = ["--quiet", "logs", "--%s" % logtype, "%s/%s" % (self.run_id, step)]
        return self.run_cli(cmd, capture_output=True).decode("utf-8")
