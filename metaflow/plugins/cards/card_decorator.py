import json
import os
import re
import tempfile
from typing import Tuple, Dict

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum
from metaflow.metaflow_current import current
from metaflow.user_configs.config_options import ConfigInput
from metaflow.user_configs.config_parameters import dump_config_values
from metaflow.util import to_unicode

from .component_serializer import CardComponentCollector, get_card_class
from .card_creator import CardCreator
from .exception import CARD_ID_PATTERN, TYPE_CHECK_REGEX

ASYNC_TIMEOUT = 30


def warning_message(message, logger=None, ts=False):
    msg = "[@card WARNING] %s" % message
    if logger:
        logger(msg, timestamp=ts, bad=True)


class MetadataStateManager(object):
    def __init__(self, info_func):
        self._info_func = info_func
        self._metadata_registered = {}

    def register_metadata(self, card_uuid) -> Tuple[bool, Dict]:
        info = self._info_func()
        # Check that metadata was not written yet. We only want to write once.
        if (
            info is None
            or info.get(card_uuid) is None
            or self._metadata_registered.get(card_uuid)
        ):
            return False, {}
        self._metadata_registered[card_uuid] = True
        return True, info.get(card_uuid)


class CardDecorator(StepDecorator):
    """
    Creates a human-readable report, a Metaflow Card, after this step completes.

    Note that you may add multiple `@card` decorators in a step with different parameters.

    Parameters
    ----------
    type : str, default 'default'
        Card type.
    id : str, optional, default None
        If multiple cards are present, use this id to identify this card.
    options : Dict[str, Any], default {}
        Options passed to the card. The contents depend on the card type.
    timeout : int, default 45
        Interrupt reporting if it takes more than this many seconds.

    MF Add To Current
    -----------------
    card -> metaflow.plugins.cards.component_serializer.CardComponentCollector
        The `@card` decorator makes the cards available through the `current.card`
        object. If multiple `@card` decorators are present, you can add an `ID` to
        distinguish between them using `@card(id=ID)` as the decorator. You will then
        be able to access that specific card using `current.card[ID].

        Methods available are `append` and `extend`

        @@ Returns
        -------
        CardComponentCollector
            The or one of the cards attached to this step.
    """

    _GLOBAL_CARD_INFO = {}

    name = "card"
    defaults = {
        "type": "default",
        "options": {},
        "scope": "task",
        "rank": None,  # Can be one of "high", "medium", "low". Can help derive ordering on the UI.
        "timeout": 45,
        "id": None,
        "save_errors": True,
        "customize": False,
        "refresh_interval": 5,
    }
    allow_multiple = True

    total_decos_on_step = {}

    step_counter = 0

    _called_once = {}

    card_creator = None

    _config_values = None

    _config_file_name = None

    task_finished_decos = 0

    def __init__(self, *args, **kwargs):
        super(CardDecorator, self).__init__(*args, **kwargs)
        self._task_datastore = None
        self._environment = None
        self._metadata = None
        self._logger = None
        self._is_editable = False
        self._card_uuid = None
        self._user_set_card_id = None
        self._metadata_registered = False

    @classmethod
    def _set_card_creator(cls, card_creator):
        cls.card_creator = card_creator

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

    @classmethod
    def _increment_completed_counter(cls):
        cls.task_finished_decos += 1

    @classmethod
    def _set_config_values(cls, config_values):
        cls._config_values = config_values

    @classmethod
    def _set_config_file_name(cls, flow):
        # Only create a config file from the very first card decorator.
        if cls._config_values and not cls._config_file_name:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", delete=False
            ) as config_file:
                config_value = dump_config_values(flow)
                json.dump(config_value, config_file)
                cls._config_file_name = config_file.name

    @classmethod
    def _register_card_info(cls, **kwargs):
        if not kwargs.get("card_uuid"):
            raise ValueError("card_uuid is required")
        cls._GLOBAL_CARD_INFO[kwargs["card_uuid"]] = kwargs

    @classmethod
    def all_cards_info(cls):
        return cls._GLOBAL_CARD_INFO.copy()

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        self._flow_datastore = flow_datastore
        self._environment = environment
        self._logger = logger

        self.card_options = None

        # We check for configuration options. We do this here before they are
        # converted to properties.
        self._set_config_values(
            [
                (config.name, ConfigInput.make_key_name(config.name))
                for _, config in flow._get_parameters()
                if config.IS_CONFIG_PARAMETER
            ]
        )

        self.card_options = self.attributes["options"]

        evt_name = "step-init"
        # `'%s-%s'%(evt_name,step_name)` ensures that we capture this once per @card per @step.
        # Since there can be many steps checking if event is registered for `evt_name` will only make it check it once for all steps.
        # Hence, we have `_is_event_registered('%s-%s'%(evt_name,step_name))`
        self._is_runtime_card = False
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
        self._task_datastore = task_datastore
        self._metadata = metadata

        # If we have configs, we need to dump them to a file so we can re-use them
        # when calling the card creation subprocess.
        # Since a step can contain multiple card decorators, and all the card creation processes
        # will reference the same config file (because of how the CardCreator is created (only single class instance)),
        # we need to ensure that a single config file is being referenced for all card create commands.
        # This config file will be removed when the last card decorator has finished creating its card.
        self._set_config_file_name(flow)
        # The MetadataStateManager is used to track the state of the metadata registration.
        # It is there to ensure that we only register metadata for the card once. This is so that we
        # avoid any un-necessary metadata writes because the create command can be called multiple times during the
        # card creation process.
        self._metadata_state_manager = MetadataStateManager(self.all_cards_info)

        card_type = self.attributes["type"]
        card_class = get_card_class(card_type)

        self._is_runtime_card = False
        if card_class is not None:  # Card type was not found
            if card_class.ALLOW_USER_COMPONENTS:
                self._is_editable = True
            self._is_runtime_card = card_class.RUNTIME_UPDATABLE

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
            self._set_card_creator(
                CardCreator(
                    self._create_top_level_args(flow),
                    self._metadata_state_manager.register_metadata,
                )
            )

            current._update_env(
                {"card": CardComponentCollector(self._logger, self.card_creator)}
            )

        # this line happens because of decospecs parsing.
        customize = False
        if str(self.attributes["customize"]) == "True":
            customize = True

        card_metadata = current.card._add_card(
            self.attributes["type"],
            self._user_set_card_id,
            self.attributes,
            self.card_options,
            editable=self._is_editable,
            customize=customize,
            runtime_card=self._is_runtime_card,
            refresh_interval=self.attributes["refresh_interval"],
        )
        self._card_uuid = card_metadata["uuid"]

        self._register_card_info(
            card_uuid=self._card_uuid,
            rank=self.attributes["rank"],
            type=self.attributes["type"],
            options=self.card_options,
            is_editable=self._is_editable,
            is_runtime_card=self._is_runtime_card,
            refresh_interval=self.attributes["refresh_interval"],
            customize=customize,
            id=self._user_set_card_id,
        )

        # This means that we are calling `task_pre_step` on the last card decorator.
        # We can now `finalize` method in the CardComponentCollector object.
        # This will set up the `current.card` object for usage inside `@step` code.
        if self.step_counter == self.total_decos_on_step[step_name]:
            current.card._finalize()

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        create_options = dict(
            card_uuid=self._card_uuid,
            user_set_card_id=self._user_set_card_id,
            runtime_card=self._is_runtime_card,
            decorator_attributes=self.attributes,
            card_options=self.card_options,
            logger=self._logger,
        )
        if is_task_ok:
            self.card_creator.create(mode="render", final=True, **create_options)
            self.card_creator.create(mode="refresh", final=True, **create_options)

        self._cleanup(step_name)

    @staticmethod
    def _options(mapping):
        for k, v in mapping.items():
            if v:
                k = k.replace("_", "-")
                v = v if isinstance(v, (list, tuple, set)) else [v]
                for value in v:
                    yield "--%s" % k
                    if not isinstance(value, bool):
                        if isinstance(value, tuple):
                            for val in value:
                                yield to_unicode(val)
                        else:
                            yield to_unicode(value)

    def _create_top_level_args(self, flow):
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
        if self._config_values:
            top_level_options["config-value"] = self._config_values
            top_level_options["local-config-file"] = self._config_file_name

        return list(self._options(top_level_options))

    def _cleanup(self, step_name):
        self._increment_completed_counter()
        if self.task_finished_decos == self.total_decos_on_step[step_name]:
            # Unlink the config file if it exists
            if self._config_file_name:
                try:
                    os.unlink(self._config_file_name)
                except Exception as e:
                    pass
