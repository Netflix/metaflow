import subprocess
import os
import sys
import json
from metaflow.decorators import StepDecorator, flow_decorators
from metaflow.current import current
from metaflow.util import to_unicode

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
        self._logger = None
        # todo : first allow multiple decorators with a step

    def add_to_package(self):
        return list(self._load_card_package())

    def _load_card_package(self):

        from . import card_modules

        card_modules_root = os.path.dirname(card_modules.__file__)

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

                # TODO: This prevents redundant packaging of .py files for the
                # default card. We should fix this logic to allow .py files to
                # be included for custom cards.
                if any(fname.endswith(s) for s in [".html", ".js", ".css"]):
                    p = os.path.join(path, fname)
                    yield p, p[prefixlen:]

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):

        self.card_options = None

        # Populate the defaults which may be missing.
        missing_keys = set(self.defaults.keys()) - set(self.attributes.keys())
        for k in missing_keys:
            self.attributes[k] = self.defaults[k]

        # when instantiation happens from the CLI we sometimes get stringified JSON and sometimes a dict for the
        # `options` attributes. Hence we need to check for both and serialized.
        if type(self.attributes["options"]) is str:
            try:
                self.card_options = json.loads(self.attributes["options"])
            except json.decoder.JSONDecodeError:
                self.card_options = self.defaults["options"]
        else:
            self.card_options = self.attributes["options"]

        self._flow_datastore = flow_datastore
        self._environment = environment
        self._logger = logger

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
        self._task_datastore = task_datastore
        self._metadata = metadata

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        if not is_task_ok:
            return
        runspec = "/".join([current.run_id, current.step_name, current.task_id])
        self._run_cards_subprocess(runspec)

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

    def _run_cards_subprocess(self, runspec):
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
        if self.card_options is not None and len(self.card_options) > 0:
            cmd += ["--options", json.dumps(self.card_options)]
        # set the id argument.

        if self.attributes["timeout"] is not None:
            cmd += ["--timeout", str(self.attributes["timeout"])]

        if self.attributes["save_errors"]:
            cmd += ["--render-error-card"]

        response, fail = self._run_command(
            cmd, os.environ, timeout=self.attributes["timeout"]
        )
        if fail:
            resp = "" if response is None else response.decode("utf-8")
            self._logger(
                "Card render failed with error : \n\n %s" % resp,
                timestamp=False,
                bad=True,
            )

    def _run_command(self, cmd, env, timeout=None):
        fail = False
        timeout_args = {}
        if timeout is not None:
            timeout_args = dict(timeout=int(timeout) + 10)
        try:
            rep = subprocess.check_output(
                cmd, env=env, stderr=subprocess.STDOUT, **timeout_args
            )
        except subprocess.CalledProcessError as e:
            rep = e.output
            fail = True
        except subprocess.TimeoutExpired as e:
            rep = e.output
            fail = True
        return rep, fail
