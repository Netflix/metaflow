import sys
import subprocess
import json
from collections import namedtuple
from tempfile import NamedTemporaryFile

from metaflow.includefile import IncludedFile
from metaflow.util import is_stringish

from . import (
    MetaflowCheck,
    AssertArtifactFailed,
    AssertLogFailed,
    truncate,
    AssertCardFailed,
)

try:
    # Python 2
    import cPickle as pickle
except:
    # Python 3
    import pickle


class CliCheck(MetaflowCheck):
    def run_cli(self, args):
        cmd = [sys.executable, "test_flow.py"]

        # remove --quiet from top level options to capture output from echo
        # we will add --quiet in args if needed
        cmd.extend([opt for opt in self.cli_options if opt != "--quiet"])
        cmd.extend(args)

        return subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )

    def assert_artifact(self, step, name, value, fields=None):
        for task, artifacts in self.artifact_dict(step, name).items():
            if name in artifacts:
                artifact = artifacts[name]
                if fields:
                    for field, v in fields.items():
                        if is_stringish(artifact):
                            data = json.loads(artifact)
                        elif isinstance(artifact, IncludedFile):
                            data = json.loads(artifact.descriptor)
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

    def assert_card(
        self,
        step,
        task,
        card_type,
        value,
        card_hash=None,
        card_id=None,
        exact_match=True,
    ):
        from metaflow.plugins.cards.exception import CardNotPresentException

        no_card_found_message = CardNotPresentException.headline
        try:
            card_data = self.get_card(
                step, task, card_type, card_hash=card_hash, card_id=card_id
            )
        except subprocess.CalledProcessError as e:
            if no_card_found_message in e.stderr.decode("utf-8").strip():
                card_data = None
            else:
                raise e
        if (exact_match and card_data != value) or (
            not exact_match and value not in card_data
        ):
            raise AssertCardFailed(
                "Task '%s/%s' expected %s card with content '%s' but got '%s'"
                % (self.run_id, step, card_type, repr(value), repr(card_data))
            )
        return True

    def list_cards(self, step, task, card_type=None):
        from metaflow.plugins.cards.exception import CardNotPresentException

        no_card_found_message = CardNotPresentException.headline
        try:
            card_data = self._list_cards(step, task=task, card_type=card_type)
        except subprocess.CalledProcessError as e:
            if no_card_found_message in e.stderr.decode("utf-8").strip():
                card_data = None
            else:
                raise e
        return card_data

    def _list_cards(self, step, task=None, card_type=None):
        with NamedTemporaryFile(dir=".") as f:
            pathspec = "%s/%s" % (self.run_id, step)
            if task is not None:
                pathspec = "%s/%s/%s" % (self.run_id, step, task)
            cmd = ["--quiet", "card", "list", pathspec, "--as-json", "--file", f.name]
            if card_type is not None:
                cmd.extend(["--type", card_type])

            self.run_cli(cmd)
            with open(f.name, "r") as jsf:
                return json.load(jsf)

    def get_card(self, step, task, card_type, card_hash=None, card_id=None):
        with NamedTemporaryFile(dir=".") as f:
            cmd = [
                "--quiet",
                "card",
                "get",
                "%s/%s/%s" % (self.run_id, step, task),
                f.name,
                "--type",
                card_type,
            ]

            if card_hash is not None:
                cmd.extend(["--hash", card_hash])
            if card_id is not None:
                cmd.extend(["--id", card_id])

            self.run_cli(cmd)
            with open(f.name, "r") as jsf:
                return jsf.read()

    def get_log(self, step, logtype):
        cmd = ["--quiet", "logs", "--%s" % logtype, "%s/%s" % (self.run_id, step)]
        completed_process = self.run_cli(cmd)
        return completed_process.stdout.decode("utf-8")

    def get_user_tags(self):
        completed_process = self.run_cli(
            ["tag", "list", "--flat", "--hide-system-tags", "--run-id", self.run_id]
        )
        lines = completed_process.stderr.decode("utf-8").splitlines()[1:]
        return frozenset(lines)

    def get_system_tags(self):
        completed_process = self.run_cli(
            ["tag", "list", "--flat", "--run-id", self.run_id]
        )
        lines = completed_process.stderr.decode("utf-8").splitlines()[1:]
        return frozenset(lines) - self.get_user_tags()

    def add_tag(self, tag):
        self.run_cli(["tag", "add", "--run-id", self.run_id, tag])

    def add_tags(self, tags):
        self.run_cli(["tag", "add", "--run-id", self.run_id, *tags])

    def remove_tag(self, tag):
        self.run_cli(["tag", "remove", "--run-id", self.run_id, tag])

    def remove_tags(self, tags):
        self.run_cli(["tag", "remove", "--run-id", self.run_id, *tags])

    def replace_tag(self, tag_to_remove, tag_to_add):
        self.run_cli(
            ["tag", "replace", "--run-id", self.run_id, tag_to_remove, tag_to_add]
        )

    def replace_tags(self, tags_to_remove, tags_to_add):
        cmd = ["tag", "replace", "--run-id", self.run_id]
        for tag_to_remove in tags_to_remove:
            cmd.extend(["--remove", tag_to_remove])
        for tag_to_add in tags_to_add:
            cmd.extend(["--add", tag_to_add])
        self.run_cli(cmd)
