import time
import subprocess
import tempfile
import json
import sys
import os
from metaflow import current
from typing import Callable, Tuple, Dict


ASYNC_TIMEOUT = 30


class CardProcessManager:
    """
    This class is responsible for managing the card creation processes.

    """

    async_card_processes = {
        # "carduuid": {
        #     "proc": subprocess.Popen,
        #     "started": time.time()
        # }
    }

    @classmethod
    def _register_card_process(cls, carduuid, proc):
        cls.async_card_processes[carduuid] = {
            "proc": proc,
            "started": time.time(),
        }

    @classmethod
    def _get_card_process(cls, carduuid):
        proc_dict = cls.async_card_processes.get(carduuid, None)
        if proc_dict is not None:
            return proc_dict["proc"], proc_dict["started"]
        return None, None

    @classmethod
    def _remove_card_process(cls, carduuid):
        if carduuid in cls.async_card_processes:
            cls.async_card_processes[carduuid]["proc"].kill()
            del cls.async_card_processes[carduuid]


class CardCreator:
    def __init__(
        self,
        top_level_options,
        should_save_metadata_lambda: Callable[[str], Tuple[bool, Dict]],
    ):
        # should_save_metadata_lambda is a lambda that provides a flag to indicate if
        # card metadata should be written to the metadata store.
        # It gets called only once when the card is created inside the subprocess.
        # The intent is that this is a stateful lambda that will ensure that we only end
        # up writing to the metadata store once.
        self._top_level_options = top_level_options
        self._should_save_metadata = should_save_metadata_lambda

    def create(
        self,
        card_uuid=None,
        user_set_card_id=None,
        runtime_card=False,
        decorator_attributes=None,
        card_options=None,
        logger=None,
        mode="render",
        final=False,
        sync=False,
    ):
        # Setting `final` will affect the Reload token set during the card refresh
        # data creation along with synchronous execution of subprocess.
        # Setting `sync` will only cause synchronous execution of subprocess.
        save_metadata = False
        metadata_dict = {}
        if mode != "render" and not runtime_card:
            # silently ignore runtime updates for cards that don't support them
            return
        elif mode == "refresh":
            # don't serialize components, which can be a somewhat expensive operation,
            # if we are just updating data
            component_strings = []
        else:
            component_strings = current.card._serialize_components(card_uuid)
            # Since the mode is a render, we can check if we need to write to the metadata store.
            save_metadata, metadata_dict = self._should_save_metadata(card_uuid)
        data = current.card._get_latest_data(card_uuid, final=final, mode=mode)
        runspec = "/".join([current.run_id, current.step_name, current.task_id])
        self._run_cards_subprocess(
            card_uuid,
            user_set_card_id,
            mode,
            runspec,
            decorator_attributes,
            card_options,
            component_strings,
            logger,
            data,
            final=final,
            sync=sync,
            save_metadata=save_metadata,
            metadata_dict=metadata_dict,
        )

    def _run_cards_subprocess(
        self,
        card_uuid,
        user_set_card_id,
        mode,
        runspec,
        decorator_attributes,
        card_options,
        component_strings,
        logger,
        data=None,
        final=False,
        sync=False,
        save_metadata=False,
        metadata_dict=None,
    ):
        components_file = data_file = None
        wait = final or sync

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

        cmd += self._top_level_options + [
            "card",
            "create",
            runspec,
            "--delete-input-files",
            "--card-uuid",
            card_uuid,
            "--mode",
            mode,
            "--type",
            decorator_attributes["type"],
            # Add the options relating to card arguments.
            # todo : add scope as a CLI arg for the create method.
        ]
        if card_options is not None and len(card_options) > 0:
            cmd += ["--options", json.dumps(card_options)]
        # set the id argument.

        if decorator_attributes["timeout"] is not None:
            cmd += ["--timeout", str(decorator_attributes["timeout"])]

        if user_set_card_id is not None:
            cmd += ["--id", str(user_set_card_id)]

        if decorator_attributes["save_errors"]:
            cmd += ["--render-error-card"]

        if components_file is not None:
            cmd += ["--component-file", components_file.name]

        if data_file is not None:
            cmd += ["--data-file", data_file.name]

        if save_metadata:
            cmd += ["--save-metadata", json.dumps(metadata_dict)]

        response, fail = self._run_command(
            cmd,
            card_uuid,
            os.environ,
            timeout=decorator_attributes["timeout"],
            wait=wait,
        )
        if fail:
            resp = "" if response is None else response.decode("utf-8")
            logger(
                "Card render failed with error : \n\n %s" % resp,
                timestamp=False,
                bad=True,
            )

    def _wait_for_async_processes_to_finish(self, card_uuid, async_timeout):
        _async_proc, _async_started = CardProcessManager._get_card_process(card_uuid)
        while _async_proc is not None and _async_proc.poll() is None:
            if time.time() - _async_started > async_timeout:
                # This means the process has crossed the timeout and we need to kill it.
                CardProcessManager._remove_card_process(card_uuid)
                break

    def _run_command(self, cmd, card_uuid, env, wait=True, timeout=None):
        fail = False
        timeout_args = {}
        async_timeout = ASYNC_TIMEOUT
        if timeout is not None:
            async_timeout = int(timeout) + 10
            timeout_args = dict(timeout=int(timeout) + 10)

        if wait:
            self._wait_for_async_processes_to_finish(card_uuid, async_timeout)
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
            _async_proc, _async_started = CardProcessManager._get_card_process(
                card_uuid
            )
            if _async_proc and _async_proc.poll() is None:
                if time.time() - _async_started > async_timeout:
                    CardProcessManager._remove_card_process(card_uuid)
                    # Since we have removed the card process, we are free to run a new one
                    # This will also ensure that when a old process is removed a new one is replaced.
                    return self._run_command(
                        cmd, card_uuid, env, wait=wait, timeout=timeout
                    )
                else:
                    # silently refuse to run an async process if a previous one is still running
                    # and timeout hasn't been reached
                    return "".encode(), False
            else:
                CardProcessManager._register_card_process(
                    card_uuid,
                    subprocess.Popen(
                        cmd,
                        env=env,
                        stderr=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                    ),
                )
                return "".encode(), False
