import subprocess
import os
import sys
import json
import tempfile
from metaflow.decorators import StepDecorator, flow_decorators
from metaflow.current import current
from metaflow.util import to_unicode
from .card_modules import serialize_components

# from metaflow import get_metadata
import re

CARD_ID_PATTERN = re.compile(
    "^[a-zA-Z0-9_]+$",
)


class CardDecorator(StepDecorator):
    name = "card"
    defaults = {
        "type": "default",
        "options": {},
        "scope": "task",
        "timeout": 45,
        "save_errors": True,
    }

    def __init__(self, *args, **kwargs):
        super(CardDecorator, self).__init__(*args, **kwargs)
        self._task_datastore = None
        self._environment = None
        self._metadata = None
        # todo : first allow multiple decorators with a step

    def add_to_package(self):
        return list(self._load_card_package())

    def _load_card_package(self):
        try:
            import metaflow_cards
        except ImportError:
            metaflow_cards_root = None
        else:
            metaflow_cards_root = os.path.dirname(metaflow_cards.__file__)

        from . import card_modules

        card_modules_root = os.path.dirname(card_modules.__file__)

        if metaflow_cards_root:
            # What if a file is too large and
            # gets tagged along the metaflow_cards
            # path; In such cases we can have huge tarballs
            # that get created;
            # Should we have package suffixes added over here?
            for path_tuple in self._walk(metaflow_cards_root):
                yield path_tuple

        for path_tuple in self._walk(card_modules_root):
            file_path, arcname = path_tuple
            yield (file_path, os.path.join("metaflow", "plugins", "cards", arcname))

    def _walk(self, root):
        root = to_unicode(root)  # handle files/folder with non ascii chars
        prefixlen = len("%s/" % os.path.dirname(root))
        for path, dirs, files in os.walk(root):
            for fname in files:
                # ignoring filesnames which are hidden;
                # TODO : Should we ignore hidden filenames
                if fname[0] == ".":
                    continue

                p = os.path.join(path, fname)
                yield p, p[prefixlen:]

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        # We do this because of Py3 support JSONDecodeError and
        # Py2 raises ValueError
        # https://stackoverflow.com/questions/53355389/python-2-3-compatibility-issue-with-exception
        try:
            import json

            RaisingError = json.decoder.JSONDecodeError
        except AttributeError:  # Python 2
            RaisingError = ValueError

        self.card_options = None

        # Populate the defaults which may be missing.
        missing_keys = set(self.defaults.keys()) - set(self.attributes.keys())
        for k in missing_keys:
            self.attributes[k] = self.defaults[k]

        if type(self.attributes["options"]) is str:
            try:
                self.card_options = json.loads(self.attributes["options"])
            except RaisingError:
                self.card_options = self.defaults["options"]
        else:
            self.card_options = self.attributes["options"]

        self._flow_datastore = flow_datastore
        self._environment = environment

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        from functools import partial

        self._task_datastore = task_datastore
        current._update_env({"card": []})
        self._metadata = metadata

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        component_strings = []
        # Upon task finish we render cards which are held in current.
        # The `serialize_components` function safely tries to render each component
        # These components are passed down to the next card subprocess via a temp JSON file
        if len(current.card) > 0:
            component_strings = serialize_components(current.card)

        if not is_task_ok:
            # todo : What do we do when underlying `step` soft-fails.
            # Todo : What do we do when underlying `@card` fails in some way?
            return
        runspec = "/".join([current.run_id, current.step_name, current.task_id])
        self._run_cards_subprocess(runspec, component_strings)

    @staticmethod
    def _options(mapping):
        for k, v in mapping.items():
            if v:
                k = k.replace("_", "-")
                v = v if isinstance(v, (list, tuple, set)) else [v]
                for value in v:
                    yield "--%s" % k
                    if not isinstance(value, bool):
                        yield to_unicode(value)

    def _create_top_level_args(self):

        top_level_options = {
            "quiet": True,
            "metadata": self._metadata.TYPE,
            "coverage": "coverage" in sys.modules,
            "environment": self._environment.TYPE,
            "datastore": self._flow_datastore.TYPE,
            "datastore-root": self._flow_datastore.datastore_root,
            # We don't provide --with as all execution is taking place in
            # the context of the main processs
        }
        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook.
        for deco in flow_decorators():
            top_level_options.update(deco.get_top_level_options())
        return list(self._options(top_level_options))

    def _run_cards_subprocess(self, runspec, component_strings):
        temp_file = None
        if len(component_strings) > 0:
            temp_file = tempfile.NamedTemporaryFile("w", suffix=".json")
            json.dump(component_strings, temp_file)
            temp_file.seek(0)

        executable = sys.executable
        cmd = [
            executable,
            sys.argv[0],
        ]
        cmd += self._create_top_level_args() + [
            "card",
            "create",
            runspec,
            "--type",
            self.attributes["type"],
            # Add the options relating to card arguments.
            # todo : add scope as a CLI arg for the create method.
        ]
        if temp_file is not None:
            cmd += ["--component-file", temp_file.name]
        if self.card_options is not None and len(self.card_options) > 0:
            cmd += ["--options", json.dumps(self.card_options)]
        # set the id argument.

        if self.attributes["timeout"] is not None:
            cmd += ["--timeout", str(self.attributes["timeout"])]

        if self.attributes["save_errors"]:
            cmd += ["--with-error-card"]

        response, fail = self._run_command(cmd, os.environ)
        if fail:
            # todo : Handle failure scenarios better.
            print("Process Failed", response.decode("utf-8"))

    def _run_command(self, cmd, env):
        fail = False
        try:
            rep = subprocess.check_output(
                cmd,
                env=env,
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as e:
            rep = e.output
            fail = True
        return rep, fail
