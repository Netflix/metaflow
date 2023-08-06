import subprocess
import os
import tempfile
import sys
import time
import json

from typing import Dict, Any

from metaflow.decorators import StepDecorator, flow_decorators
from metaflow.current import current
from metaflow.util import to_unicode
from .component_serializer import CardComponentCollector, get_card_class


# from metaflow import get_metadata
import re

from .exception import CARD_ID_PATTERN, TYPE_CHECK_REGEX

ASYNC_TIMEOUT = 30


def warning_message(message, logger=None, ts=False):
    msg = "[@card WARNING] %s" % message
    if logger:
        logger(msg, timestamp=ts, bad=True)


class CardDecorator(StepDecorator):
    """
    Creates a human-readable report, a Metaflow Card, after this step completes.

    Note that you may add multiple `@card` decorators in a step with different parameters.

    Parameters
    ----------
    type : str, default: 'default'
        Card type.
    id : str, optional, default: None
        If multiple cards are present, use this id to identify this card.
    options : Dict[str, Any], default: {}
        Options passed to the card. The contents depend on the card type.
    timeout : int, default: 45
        Interrupt reporting if it takes more than this many seconds.
    """

    name = "card"
    defaults = {
        "type": "default",
        "options": {},
        "scope": "task",
        "timeout": 45,
        "id": None,
        "save_errors": True,
        "customize": False,
    }
    allow_multiple = True

    total_decos_on_step = {}

    step_counter = 0

    _called_once = {}

    def __init__(self, *args, **kwargs):
        super(CardDecorator, self).__init__(*args, **kwargs)
        self._task_datastore = None
        self._environment = None
        self._metadata = None
        self._logger = None
        self._is_editable = False
        self._card_uuid = None
        self._user_set_card_id = None
        self._async_proc = None

    def _is_event_registered(self, evt_name):
        return evt_name in self._called_once

    @classmethod
    def _register_event(cls, evt_name):
        if evt_name not in cls._called_once:
            cls._called_once[evt_name] = True

    @classmethod
    def _set_card_counts_per_step(cls, step_name, total_count):
        cls.total_decos_on_step[step_name] = total_count

    @classmethod
    def _increment_step_counter(cls):
        cls.step_counter += 1

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        self._flow_datastore = flow_datastore
        self._environment = environment
        self._logger = logger
        self.card_options = None

        self.card_options = self.attributes["options"]

        evt_name = "step-init"
        # `'%s-%s'%(evt_name,step_name)` ensures that we capture this once per @card per @step.
        # Since there can be many steps checking if event is registered for `evt_name` will only make it check it once for all steps.
        # Hence, we have `_is_event_registered('%s-%s'%(evt_name,step_name))`
        evt = "%s-%s" % (evt_name, step_name)
        if not self._is_event_registered(evt):
            # We set the total count of decorators so that we can use it for
            # when calling the finalize function of CardComponentCollector
            # We set the total @card per step via calling `_set_card_counts_per_step`.
            other_card_decorators = [
                deco for deco in decorators if isinstance(deco, self.__class__)
            ]
            self._set_card_counts_per_step(step_name, len(other_card_decorators))
            self._register_event(evt)

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
        card_type = self.attributes["type"]
        card_class = get_card_class(card_type)

        self._is_runtime_card = False
        if card_class is not None:  # Card type was not found
            if card_class.ALLOW_USER_COMPONENTS:
                self._is_editable = True
            self._is_runtime_card = card_class.IS_RUNTIME_CARD

        # We have a step counter to ensure that on calling the final card decorator's `task_pre_step`
        # we call a `finalize` function in the `CardComponentCollector`.
        # This can help ensure the behaviour of the `current.card` object is according to specification.
        self._increment_step_counter()
        self._user_set_card_id = self.attributes["id"]
        if (
            self.attributes["id"] is not None
            and re.match(CARD_ID_PATTERN, self.attributes["id"]) is None
        ):
            # There should be a warning issued to the user that `id` doesn't match regex pattern
            # Since it is doesn't match pattern, we need to ensure that `id` is not accepted by `current`
            # and warn users that they cannot use id for their arguments.
            wrn_msg = (
                "@card with id '%s' doesn't match REGEX pattern. "
                "Adding custom components to cards will not be accessible via `current.card['%s']`. "
                "Please create `id` of pattern %s. "
            ) % (self.attributes["id"], self.attributes["id"], TYPE_CHECK_REGEX)
            warning_message(wrn_msg, self._logger)
            self._user_set_card_id = None

        # As we have multiple decorators,
        # we need to ensure that `current.card` has `CardComponentCollector` instantiated only once.
        if not self._is_event_registered("pre-step"):
            self._register_event("pre-step")
            current._update_env(
                {"card": CardComponentCollector(self._logger, self._card_proc)}
            )

        # this line happens because of decospecs parsing.
        customize = False
        if str(self.attributes["customize"]) == "True":
            customize = True

        card_metadata = current.card._add_card(
            self.attributes["type"],
            self._user_set_card_id,
            self._is_editable,
            customize,
        )
        self._card_uuid = card_metadata["uuid"]

        # This means that we are calling `task_pre_step` on the last card decorator.
        # We can now `finalize` method in the CardComponentCollector object.
        # This will set up the `current.card` object for usage inside `@step` code.
        if self.step_counter == self.total_decos_on_step[step_name]:
            current.card._finalize()

        self._task_datastore = task_datastore
        self._metadata = metadata

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        if is_task_ok:
            self._card_proc("render")
            self._card_proc("refresh", final=True)

    def _card_proc(self, mode, final=False):
        if mode != "render" and not self._is_runtime_card:
            # silently ignore runtime updates for cards that don't support them
            return
        elif mode == "refresh":
            # don't serialize components, which can be a somewhat expensive operation,
            # if we are just updating data
            component_strings = []
        else:
            component_strings = current.card._serialize_components(self._card_uuid)

        data = current.card._get_latest_data(self._card_uuid, final=final)
        runspec = "/".join([current.run_id, current.step_name, current.task_id])
        self._run_cards_subprocess(mode, runspec, component_strings, data)

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
            "environment": self._environment.TYPE,
            "datastore": self._flow_datastore.TYPE,
            "datastore-root": self._flow_datastore.datastore_root,
            "no-pylint": True,
            "event-logger": "nullSidecarLogger",
            "monitor": "nullSidecarMonitor",
            # We don't provide --with as all execution is taking place in
            # the context of the main process
        }
        return list(self._options(top_level_options))

    def _run_cards_subprocess(self, mode, runspec, component_strings, data=None):
        components_file = data_file = None
        wait = mode == "render"

        if len(component_strings) > 0:
            # note that we can't delete temporary files here when calling the subprocess
            # async due to a race condition. The subprocess must delete them
            components_file = tempfile.NamedTemporaryFile(
                "w", suffix=".json", delete=False
            )
            json.dump(component_strings, components_file)
            components_file.seek(0)
        if data is not None:
            data_file = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
            json.dump(data, data_file)
            data_file.seek(0)

        executable = sys.executable
        cmd = [
            executable,
            sys.argv[0],
        ]
        cmd += self._create_top_level_args() + [
            "card",
            "create",
            runspec,
            "--delete-input-files",
            "--card-uuid",
            self._card_uuid,
            "--mode",
            mode,
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

        if self._user_set_card_id is not None:
            cmd += ["--id", str(self._user_set_card_id)]

        if self.attributes["save_errors"]:
            cmd += ["--render-error-card"]

        if components_file is not None:
            cmd += ["--component-file", components_file.name]

        if data_file is not None:
            cmd += ["--data-file", data_file.name]

        response, fail = self._run_command(
            cmd, os.environ, timeout=self.attributes["timeout"], wait=wait
        )
        if fail:
            resp = "" if response is None else response.decode("utf-8")
            self._logger(
                "Card render failed with error : \n\n %s" % resp,
                timestamp=False,
                bad=True,
            )

    def _run_command(self, cmd, env, wait=True, timeout=None):
        fail = False
        timeout_args = {}
        async_timeout = ASYNC_TIMEOUT
        if timeout is not None:
            async_timeout = int(timeout) + 10
            timeout_args = dict(timeout=int(timeout) + 10)

        if wait:
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
        else:
            if self._async_proc and self._async_proc.poll() is None:
                if time.time() - self._async_started > async_timeout:
                    self._async_proc.kill()
                else:
                    # silently refuse to run an async process if a previous one is still running
                    # and timeout hasn't been reached
                    return "", False
            else:
                # print("CARD CMD", " ".join(cmd))
                self._async_proc = subprocess.Popen(
                    cmd, env=env, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL
                )
                self._async_started = time.time()
                return "", False
