from .card_modules import MetaflowCardComponent
from .card_modules.basic import ErrorComponent, SectionComponent
from .card_modules.components import (
    UserComponent,
    create_component_id,
    StubComponent,
)
from .exception import ComponentOverwriteNotSupportedException
from metaflow.metaflow_config import RUNTIME_CARD_RENDER_INTERVAL
import uuid
import json
import platform
from collections import OrderedDict
import time

_TYPE = type


def get_card_class(card_type):
    from metaflow.plugins import CARDS

    filtered_cards = [card for card in CARDS if card.type == card_type]
    if len(filtered_cards) == 0:
        return None
    return filtered_cards[0]


def _component_is_valid(component):
    """
    Validates if the component is of the correct class.
    """
    return issubclass(type(component), MetaflowCardComponent)


def warning_message(message, logger=None, ts=False):
    if logger:
        msg = "[@card WARNING] %s" % message
        logger(msg, timestamp=ts, bad=True)


class WarningComponent(ErrorComponent):
    def __init__(self, warning_message):
        super().__init__("@card WARNING", warning_message)


class ComponentStore:
    """
    The `ComponentStore` object helps store the components for a single card in memory.
    This class has combination of a array/dictionary like interfaces to access/change the stored components.

    It exposes the `append` /`extend` methods (like an array) to add components.
    It also exposes the `__getitem__`/`__setitem__` methods (like a dictionary) to access the components by their Ids.
    """

    def _set_component_map(self):
        """
        The `_component_map` attribute is supposed to be a dictionary so that we can access the components by their ids.
        But we also want to maintain order in which components are inserted since all of these components are going to be visible on a UI.
        Since python3.6 dictionaries are ordered by default so we can use the default python `dict`.
        For python3.5 and below we need to use an OrderedDict since `dict`'s are not ordered by default.
        """
        python_version = int(platform.python_version_tuple()[0]) * 10 + int(
            platform.python_version_tuple()[1]
        )
        if python_version < 36:
            self._component_map = OrderedDict()
        else:
            self._component_map = {}

    def __init__(self, logger, card_type=None, components=None, user_set_id=None):
        self._logger = logger
        self._card_type = card_type
        self._user_set_id = user_set_id
        self._layout_last_changed_on = time.time()
        self._set_component_map()
        if components is not None:
            for c in list(components):
                self._store_component(c, component_id=None)

    @property
    def layout_last_changed_on(self):
        """This property helps the CardComponentManager identify when the layout of the card has changed so that it can trigger a re-render of the card."""
        return self._layout_last_changed_on

    def _realtime_updateable_components(self):
        for c in self._component_map.values():
            if c.REALTIME_UPDATABLE:
                yield c

    def _store_component(self, component, component_id=None):
        if not _component_is_valid(component):
            warning_message(
                "Component (%s) is not a valid MetaflowCardComponent. It will not be stored."
                % str(component),
                self._logger,
            )
            return
        if component_id is not None:
            component.component_id = component_id
        elif component.component_id is None:
            component.component_id = create_component_id(component)
        setattr(component, "_logger", self._logger)
        self._component_map[component.component_id] = component
        self._layout_last_changed_on = time.time()

    def _remove_component(self, component_id):
        del self._component_map[component_id]
        self._layout_last_changed_on = time.time()

    def __iter__(self):
        return iter(self._component_map.values())

    def __setitem__(self, key, value):
        if self._component_map.get(key) is not None:
            # This is the equivalent of calling `current.card.components["mycomponent"] = Markdown("## New Component")`
            # We don't support the replacement of individual components in the card.
            # Instead we support rewriting the entire component array instead.
            # So users can run `current.card[ID] = [FirstComponent, SecondComponent]` which will instantiate an entirely
            # new ComponentStore.
            # So we should throw an error over here, since it is clearly an operation which is not supported.
            raise ComponentOverwriteNotSupportedException(
                key, self._user_set_id, self._card_type
            )
        else:
            self._store_component(value, component_id=key)

    def __getitem__(self, key):
        if key not in self._component_map:
            # Store a stub-component in place since `key` doesnt exist.
            # If the user does a `current.card.append(component, id=key)`
            # then the stub component will be replaced by the actual component.
            self._store_component(StubComponent(key), component_id=key)
        return self._component_map[key]

    def __delitem__(self, key):
        if key not in self._component_map:
            raise KeyError(
                "MetaflowCardComponent with id `%s` not found. Available components for the cards include : %s"
                % (key, ", ".join(self.keys()))
            )
        self._remove_component(key)

    def __contains__(self, key):
        return key in self._component_map

    def append(self, component, id=None):
        self._store_component(component, component_id=id)

    def extend(self, components):
        for c in components:
            self._store_component(c, component_id=None)

    def clear(self):
        self._component_map.clear()

    def keys(self):
        return list(self._component_map.keys())

    def values(self):
        return self._component_map.values()

    def __str__(self):
        return "Card components present in the card: `%s` " % ("`, `".join(self.keys()))

    def __len__(self):
        return len(self._component_map)


def _object_is_json_serializable(obj):
    try:
        json.dumps(obj)
        return True
    except TypeError as e:
        return False


class CardComponentManager:
    """
    This class manages the card's state for a single card.
    - It uses the `ComponentStore` to manage the storage of the components
    - It exposes methods to add, remove and access the components.
    - It exposes a `refresh` method that will allow refreshing a card with new data
    for realtime(ish) updates.
    - The `CardComponentCollector` exposes convenience methods similar to this class for a default
    editable card. These methods include :
        - `append`
        - `extend`
        - `clear`
        - `refresh`
        - `components`
        - `__iter__`

    ## Usage Patterns :

    ```python
    current.card["mycardid"].append(component, id="comp123")
    current.card["mycardid"].extend([component])
    current.card["mycardid"].refresh(data) # refreshes the card with new data
    current.card["mycardid"].components["comp123"] # returns the component with id "comp123"
    current.card["mycardid"].components["comp123"].update()
    current.card["mycardid"].components.clear() # Wipe all the components
    del current.card["mycardid"].components["mycomponentid"] # Delete a component
    ```
    """

    def __init__(
        self,
        card_uuid,
        decorator_attributes,
        card_creator,
        components=None,
        logger=None,
        no_warnings=False,
        user_set_card_id=None,
        runtime_card=False,
        card_options=None,
        refresh_interval=5,
    ):
        self._card_creator_args = dict(
            card_uuid=card_uuid,
            user_set_card_id=user_set_card_id,
            runtime_card=runtime_card,
            decorator_attributes=decorator_attributes,
            card_options=card_options,
            logger=logger,
        )
        self._card_creator = card_creator
        self._refresh_interval = refresh_interval
        self._last_layout_change = None
        self._latest_user_data = None
        self._last_refresh = 0
        self._last_render = 0
        self._render_seq = 0
        self._logger = logger
        self._no_warnings = no_warnings
        self._warn_once = {
            "update": {},
            "not_implemented": {},
        }
        card_type = decorator_attributes["type"]

        if components is None:
            self._components = ComponentStore(
                logger=self._logger,
                card_type=card_type,
                user_set_id=user_set_card_id,
                components=None,
            )
        else:
            self._components = ComponentStore(
                logger=self._logger,
                card_type=card_type,
                user_set_id=user_set_card_id,
                components=list(components),
            )

    def append(self, component, id=None):
        self._components.append(component, id=id)

    def extend(self, components):
        self._components.extend(components)

    def clear(self):
        self._components.clear()

    def _card_proc(self, mode, sync=False):
        self._card_creator.create(**self._card_creator_args, mode=mode, sync=sync)

    def refresh(self, data=None, force=False):
        self._latest_user_data = data
        nu = time.time()
        first_render = True if self._last_render == 0 else False

        if nu - self._last_refresh < self._refresh_interval:
            # rate limit refreshes: silently ignore requests that
            # happen too frequently
            return
        self._last_refresh = nu

        # This block of code will render the card in `render_runtime` mode when:
        # 1. refresh is called with `force=True`
        # 2. Layout of the components in the card has changed. i.e. The actual elements in the component array have changed.
        # 3. The last time the card was rendered was more the minimum interval after which they should be rendered.
        last_rendered_before_minimum_interval = (
            nu - self._last_refresh
        ) > RUNTIME_CARD_RENDER_INTERVAL
        layout_has_changed = (
            self._last_layout_change != self.components.layout_last_changed_on
            or self._last_layout_change is None
        )
        if force or last_rendered_before_minimum_interval or layout_has_changed:
            self._render_seq += 1
            self._last_render = nu
            self._card_proc("render_runtime")
            # The below `if not first_render` condition is a special case for the following scenario:
            # Lets assume the case that the user is only doing `current.card.append` followed by `refresh`.
            # In this case, there will be no process executed in `refresh` mode since `layout_has_changed`
            # will always be true and as a result there will be no data update that informs the UI of the RELOAD_TOKEN change.
            # This will cause the UI to seek for the data update object but will constantly find None. So if it is not
            # the first render then we should also have a `refresh` call followed by a `render_runtime` call so
            # that the UI can always be updated with the latest data.
            if not first_render:
                # For the general case, the CardCreator's ProcessManager run's the `refresh` / `render_runtime` in a asynchronous manner.
                # Due to this when the `render_runtime` call is happening, an immediately subsequent call to `refresh` will not be able to
                # execute since the card-process manager will be busy executing the `render_runtime` call and ignore the `refresh` call.
                # Hence we need to pass the `sync=True` argument to the `refresh` call so that the `refresh` call is executed synchronously and waits for the
                # `render_runtime` call to finish.
                self._card_proc("refresh", sync=True)
            # We set self._last_layout_change so that when self._last_layout_change is not the same
            # as `self.components.layout_last_changed_on`, then the component array itself
            # has been modified. So we should force a re-render of the card.
            self._last_layout_change = self.components.layout_last_changed_on
        else:
            self._card_proc("refresh")

    @property
    def components(self):
        return self._components

    def _warning(self, message):
        msg = "[@card WARNING] %s" % message
        self._logger(msg, timestamp=False, bad=True)

    def _get_latest_data(self, final=False, mode=None):
        """
        This function returns the data object that is passed down to :
        - `MetaflowCard.render_runtime`
        - `MetaflowCard.refresh`
        - `MetaflowCard.reload_content_token`

        The return value of this function contains all the necessary state information for Metaflow Cards to make decisions on the following:
        1. What components are rendered
        2. Should the card be reloaded on the UI
        3. What data to pass down to the card.

        Parameters
        ----------
        final : bool, optional
            If True, it implies that the final "rendering" sequence is taking place (which involves calling a `render` and a `refresh` function.)
            When final is set the `render_seq` is set to "final" so that the reload token in the card is set to final
            and the card is not reloaded again on the user interface.
        mode : str
            This parameter is passed down to the object returned by this function. Can be one of `render_runtime` / `refresh` / `render`

        Returns
        -------
        dict
            A dictionary of the form :
            ```python
            {
                "user": user_data, # any passed to `current.card.refresh` function
                "components": component_dict, # all rendered REALTIME_UPDATABLE components
                "render_seq": seq,
                # `render_seq` is a counter that is incremented every time `render_runtime` is called.
                # If a metaflow card has a RELOAD_POLICY_ALWAYS set then the reload token will be set to this value
                # so that the card reload on the UI everytime `render_runtime` is called.
                "component_update_ts": self.components.layout_last_changed_on,
                # `component_update_ts` is the timestamp of the last time the component array was modified.
                # `component_update_ts` can get used by the `reload_content_token` to make decisions on weather to
                # reload the card on the UI when component array has changed.
                "mode": mode,
            }
            ```
        """
        seq = "final" if final else self._render_seq
        # Extract all the runtime-updatable components as a dictionary
        component_dict = {}
        for component in self._components._realtime_updateable_components():
            rendered_comp = _render_card_component(component)
            if rendered_comp is not None:
                component_dict.update({component.component_id: rendered_comp})

        # Verify _latest_user_data is json serializable
        user_data = {}
        if self._latest_user_data is not None and not _object_is_json_serializable(
            self._latest_user_data
        ):
            self._warning(
                "Data provided to `refresh` is not JSON serializable. It will be ignored."
            )
        else:
            user_data = self._latest_user_data

        return {
            "user": user_data,
            "components": component_dict,
            "render_seq": seq,
            "component_update_ts": self.components.layout_last_changed_on,
            "mode": mode,
        }

    def __iter__(self):
        return iter(self._components)


class CardComponentCollector:
    """
    This class helps collect `MetaflowCardComponent`s during runtime execution

    ### Usage with `current`
    `current.card` is of type `CardComponentCollector`

    ### Main Usage TLDR
    - [x] `current.card.append` customizes the default editable card.
    - [x] Only one card can be default editable in a step.
    - [x] The card class must have `ALLOW_USER_COMPONENTS=True` to be considered default editable.
        - [x] Classes with `ALLOW_USER_COMPONENTS=False` are never default editable.
    - [x] The user can specify an `id` argument to a card, in which case the card is editable through `current.card[id].append`.
        - [x] A card with an id can be also default editable, if there are no other cards that are eligible to be default editable.
    - [x] If multiple default-editable cards exist but only one card doesn't have an id, the card without an id is considered to be default editable.
    - [x] If we can't resolve a single default editable card through the above rules, `current.card`.append calls show a warning but the call doesn't fail.
    - [x] A card that is not default editable can be still edited through:
        - [x] its `current.card['myid']`
        - [x] by looking it up by its type, e.g. `current.card.get(type='pytorch')`.
    """

    def __init__(self, logger=None, card_creator=None):
        from metaflow.metaflow_config import CARD_NO_WARNING

        self._card_component_store = (
            # Each key in the dictionary is the UUID of an individual card.
            # value is of type `CardComponentManager`, holding a list of MetaflowCardComponents for that particular card
            {}
        )
        self._cards_meta = (
            {}
        )  # a `dict` of (card_uuid, `dict)` holding all metadata about all @card decorators on the `current` @step.
        self._card_id_map = {}  # card_id to uuid map for all cards with ids
        self._logger = logger
        self._card_creator = card_creator
        # `self._default_editable_card` holds the uuid of the card that is default editable. This card has access to `append`/`extend` methods of `self`
        self._default_editable_card = None
        self._warned_once = {
            "__getitem__": {},
            "append": False,
            "extend": False,
            "update": False,
            "update_no_id": False,
        }
        self._no_warnings = True if CARD_NO_WARNING else False

    @staticmethod
    def create_uuid():
        return str(uuid.uuid4()).replace("-", "")

    def _log(self, *args, **kwargs):
        if self._logger:
            self._logger(*args, **kwargs)

    def _add_card(
        self,
        card_type,
        card_id,
        decorator_attributes,
        card_options,
        editable=False,
        customize=False,
        suppress_warnings=False,
        runtime_card=False,
        refresh_interval=5,
    ):
        """
        This function helps collect cards from all the card decorators.
        As `current.card` is a singleton this function is called by all @card decorators over a @step to add editable cards.

        ## Parameters

            - `card_type` (str) : value of the associated `MetaflowCard.type`
            - `card_id` (str) : `id` argument provided at top of decorator
            - `editable` (bool) : this corresponds to the value of `MetaflowCard.ALLOW_USER_COMPONENTS` for that `card_type`
            - `customize` (bool) : This argument is reserved for a single @card decorator per @step.
                - An `editable` card with `customize=True` gets precedence to be set as default editable card.
                - A default editable card is the card which can be access via the `append` and `extend` methods.
        """
        card_uuid = self.create_uuid()
        card_metadata = dict(
            type=card_type,
            uuid=card_uuid,
            card_id=card_id,
            editable=editable,
            customize=customize,
            suppress_warnings=suppress_warnings,
            runtime_card=runtime_card,
            decorator_attributes=decorator_attributes,
            card_options=card_options,
            refresh_interval=refresh_interval,
        )
        self._cards_meta[card_uuid] = card_metadata
        self._card_component_store[card_uuid] = CardComponentManager(
            card_uuid,
            decorator_attributes,
            self._card_creator,
            components=None,
            logger=self._logger,
            no_warnings=self._no_warnings,
            user_set_card_id=card_id,
            runtime_card=runtime_card,
            card_options=card_options,
            refresh_interval=refresh_interval,
        )
        return card_metadata

    def _warning(self, message):
        msg = "[@card WARNING] %s" % message
        self._log(msg, timestamp=False, bad=True)

    def _add_warning_to_cards(self, warn_msg):
        if self._no_warnings:
            return
        for card_id in self._card_component_store:
            if not self._cards_meta[card_id]["suppress_warnings"]:
                self._card_component_store[card_id].append(WarningComponent(warn_msg))

    def get(self, type=None):
        """`get`
        gets all the components arrays for a card `type`.
        Since one `@step` can have many `@card` decorators, many decorators can have the same type. That is why this function returns a list of lists.

        Args:
            type ([str], optional): `type` of MetaflowCard. Defaults to None.

        Returns: will return empty `list` if `type` is None or not found
            List[List[MetaflowCardComponent]]
        """
        card_type = type
        card_uuids = [
            card_meta["uuid"]
            for card_meta in self._cards_meta.values()
            if card_meta["type"] == card_type
        ]
        return [self._card_component_store[uuid] for uuid in card_uuids]

    def _finalize(self):
        """
        The `_finalize` function is called once the last @card decorator calls `step_init`. Calling this function makes `current.card` ready for usage inside `@step` code.
        This function's works two parts :
        1. Resolving `self._default_editable_card`.
                - The `self._default_editable_card` holds the uuid of the card that will have access to the `append`/`extend` methods.
        2. Resolving edge cases where @card `id` argument may be `None` or have a duplicate `id` when there are more than one editable cards.
        3. Resolving the `self._default_editable_card` to the card with the`customize=True` argument.
        """
        all_card_meta = list(self._cards_meta.values())
        for c in all_card_meta:
            ct = get_card_class(c["type"])
            c["exists"] = False
            if ct is not None:
                c["exists"] = True

        # If a card has `customize=True` and is not editable then it will not be considered default editable.
        editable_cards_meta = [c for c in all_card_meta if c["editable"]]

        if len(editable_cards_meta) == 0:
            return

        # Create the `self._card_id_map` lookup table which maps card `id` to `uuid`.
        # This table has access to all cards with `id`s set to them.
        card_ids = []
        for card_meta in all_card_meta:
            if card_meta["card_id"] is not None:
                self._card_id_map[card_meta["card_id"]] = card_meta["uuid"]
                card_ids.append(card_meta["card_id"])

        # If there is only one editable card then this card becomes `self._default_editable_card`
        if len(editable_cards_meta) == 1:
            self._default_editable_card = editable_cards_meta[0]["uuid"]
            return

        # Segregate cards which have id as none and those which don't.
        not_none_id_cards = [c for c in editable_cards_meta if c["card_id"] is not None]
        none_id_cards = [c for c in editable_cards_meta if c["card_id"] is None]

        # If there is only 1 card with id set to None then we can use that as the default card.
        if len(none_id_cards) == 1:
            self._default_editable_card = none_id_cards[0]["uuid"]

        # If the size of the set of ids is not equal to total number of cards with ids then warn the user that we cannot disambiguate
        # so `current.card['my_card_id']` won't work.
        id_set = set(card_ids)
        if len(card_ids) != len(id_set):
            non_unique_ids = [
                idx
                for idx in id_set
                if len(list(filter(lambda x: x["card_id"] == idx, not_none_id_cards)))
                > 1
            ]
            nui = ", ".join(non_unique_ids)
            # throw a warning that decorators have non-unique Ids
            self._warning(
                (
                    "Multiple `@card` decorator have been annotated with duplicate ids : %s. "
                    "`current.card['%s']` will not work"
                )
                % (nui, non_unique_ids[0])
            )

            # remove the non unique ids from the `self._card_id_map`
            for idx in non_unique_ids:
                del self._card_id_map[idx]

        # if a @card has `customize=True` in the arguments then there should only be one @card with `customize=True`. This @card will be the _default_editable_card
        customize_cards = [c for c in editable_cards_meta if c["customize"]]
        if len(customize_cards) > 1:
            self._warning(
                (
                    "Multiple @card decorators have `customize=True`. "
                    "Only one @card per @step can have `customize=True`. "
                    "`current.card.append` will ignore all decorators marked `customize=True`."
                )
            )
        elif len(customize_cards) == 1:
            # since `editable_cards_meta` hold only `editable=True` by default we can just set this card here.
            self._default_editable_card = customize_cards[0]["uuid"]

    def __getitem__(self, key):
        """
        Choose a specific card for manipulation.

        When multiple @card decorators are present, you can add an
        `ID` to distinguish between them, `@card(id=ID)`. This allows you
        to add components to a specific card like this:
        ```
        current.card[ID].append(component)
        ```

        Parameters
        ----------
        key : str
            Card ID.

        Returns
        -------
        CardComponentManager
            An object with `append` and `extend` calls which allow you to
            add components to the chosen card.
        """
        if key in self._card_id_map:
            card_uuid = self._card_id_map[key]
            return self._card_component_store[card_uuid]
        if key not in self._warned_once["__getitem__"]:
            _warn_msg = [
                "`current.card['%s']` is not present. Please set the `id` argument in @card to '%s' to access `current.card['%s']`."
                % (key, key, key),
                "`current.card['%s']` will return an empty `list` which is not referenced to `current.card` object."
                % (key),
            ]
            self._warning(" ".join(_warn_msg))
            self._add_warning_to_cards("\n".join(_warn_msg))
            self._warned_once["__getitem__"][key] = True

        return []

    def __setitem__(self, key, value):
        """
        Specify components of the chosen card.

        Instead of adding components to a card individually with `current.card[ID].append(component)`,
        use this method to assign a list of components to a card, replacing the existing components:
        ```
        current.card[ID] = [FirstComponent, SecondComponent]
        ```

        Parameters
        ----------
        key: str
            Card ID.

        value: List[MetaflowCardComponent]
            List of card components to assign to this card.
        """
        if key in self._card_id_map:
            card_uuid = self._card_id_map[key]
            if not isinstance(value, list):
                _warning_msg = (
                    "`current.card['%s']` not set. `current.card['%s']` should be a `list` of `MetaflowCardComponent`."
                    % (key, key)
                )
                self._warning(_warning_msg)
                return
            self._card_component_store[card_uuid] = CardComponentManager(
                card_uuid,
                self._cards_meta[card_uuid]["decorator_attributes"],
                self._card_creator,
                components=value,
                logger=self._logger,
                no_warnings=self._no_warnings,
                user_set_card_id=key,
                card_options=self._cards_meta[card_uuid]["card_options"],
                runtime_card=self._cards_meta[card_uuid]["runtime_card"],
                refresh_interval=self._cards_meta[card_uuid]["refresh_interval"],
            )
            return

        self._warning(
            "`current.card['%s']` is not present. Please set the `id` argument in @card to '%s' to access `current.card['%s']`. "
            % (key, key, key)
        )

    def append(self, component, id=None):
        """
        Appends a component to the current card.

        Parameters
        ----------
        component : MetaflowCardComponent
            Card component to add to this card.
        """
        if self._default_editable_card is None:
            if (
                len(self._card_component_store) == 1
            ):  # if there is one card which is not the _default_editable_card then the card is not editable
                card_type = list(self._cards_meta.values())[0]["type"]
                if list(self._cards_meta.values())[0]["exists"]:
                    _crdwr = "Card of type `%s` is not an editable card." % card_type
                    _endwr = (
                        "Please use an editable card."  # todo : link to documentation
                    )
                else:
                    _crdwr = "Card of type `%s` doesn't exist." % card_type
                    _endwr = "Please use a card `type` which exits."  # todo : link to documentation

                _warning_msg = [
                    _crdwr,
                    "Component will not be appended and `current.card.append` will not work for any call during this runtime execution.",
                    _endwr,
                ]
            else:
                _warning_msg = [
                    "`current.card.append` cannot disambiguate between multiple editable cards.",
                    "Component will not be appended and `current.card.append` will not work for any call during this runtime execution.",
                    "To fix this set the `id` argument in all @card's when using multiple @card decorators over a single @step. ",  # todo : Add Link to documentation
                ]

            if not self._warned_once["append"]:
                self._warning(" ".join(_warning_msg))
                self._add_warning_to_cards("\n".join(_warning_msg))
                self._warned_once["append"] = True

            return
        self._card_component_store[self._default_editable_card].append(component, id=id)

    def extend(self, components):
        """
        Appends many components to the current card.

        Parameters
        ----------
        component : Iterator[MetaflowCardComponent]
            Card components to add to this card.
        """
        if self._default_editable_card is None:
            # if there is one card which is not the _default_editable_card then the card is not editable
            if len(self._card_component_store) == 1:
                card_type = list(self._cards_meta.values())[0]["type"]
                _warning_msg = [
                    "Card of type `%s` is not an editable card." % card_type,
                    "Components list will not be extended and `current.card.extend` will not work for any call during this runtime execution.",
                    "Please use an editable card",  # todo : link to documentation
                ]
            else:
                _warning_msg = [
                    "`current.card.extend` cannot disambiguate between multiple @card decorators.",
                    "Components list will not be extended and `current.card.extend` will not work for any call during this runtime execution.",
                    "To fix this set the `id` argument in all @card when using multiple @card decorators over a single @step.",  # todo : Add Link to documentation
                ]
            if not self._warned_once["extend"]:
                self._warning(" ".join(_warning_msg))
                self._add_warning_to_cards("\n".join(_warning_msg))
                self._warned_once["extend"] = True

            return

        self._card_component_store[self._default_editable_card].extend(components)

    @property
    def components(self):
        # FIXME: document
        if self._default_editable_card is None:
            if len(self._card_component_store) == 1:
                card_type = list(self._cards_meta.values())[0]["type"]
                _warning_msg = [
                    "Card of type `%s` is not an editable card." % card_type,
                    "Components list will not be updated and `current.card.components` will not work for any call during this runtime execution.",
                    "Please use an editable card",  # todo : link to documentation
                ]
            else:
                _warning_msg = [
                    "`current.card.components` cannot disambiguate between multiple @card decorators.",
                    "Components list will not be accessible and `current.card.components` will not work for any call during this runtime execution.",
                    "To fix this set the `id` argument in all @card when using multiple @card decorators over a single @step and reference `current.card[ID].components`",  # todo : Add Link to documentation
                    "to update/access the appropriate card component.",
                ]
            if not self._warned_once["components"]:
                self._warning(" ".join(_warning_msg))
                self._warned_once["components"] = True
            return

        return self._card_component_store[self._default_editable_card].components

    def clear(self):
        # FIXME: document
        if self._default_editable_card is not None:
            self._card_component_store[self._default_editable_card].clear()

    def refresh(self, *args, **kwargs):
        # FIXME: document
        if self._default_editable_card is not None:
            self._card_component_store[self._default_editable_card].refresh(
                *args, **kwargs
            )

    def _get_latest_data(self, card_uuid, final=False, mode=None):
        """
        Returns latest data so it can be used in the final render() call
        """
        return self._card_component_store[card_uuid]._get_latest_data(
            final=final, mode=mode
        )

    def _serialize_components(self, card_uuid):
        """
        This method renders components present in a card to strings/json.
        Components exposed by metaflow ensure that they render safely. If components
        don't render safely then we don't add them to the final list of serialized functions
        """
        serialized_components = []
        if card_uuid not in self._card_component_store:
            return []
        has_user_components = any(
            [
                issubclass(type(component), UserComponent)
                for component in self._card_component_store[card_uuid]
            ]
        )
        for component in self._card_component_store[card_uuid]:
            rendered_obj = _render_card_component(component)
            if rendered_obj is None:
                continue
            serialized_components.append(rendered_obj)
        if has_user_components and len(serialized_components) > 0:
            serialized_components = [
                SectionComponent(contents=serialized_components).render()
            ]
        return serialized_components


def _render_card_component(component):
    if not _component_is_valid(component):
        return None
    try:
        rendered_obj = component.render()
    except:
        return None
    else:
        if not (type(rendered_obj) == str or type(rendered_obj) == dict):
            return None
        else:
            # Since `UserComponent`s are safely_rendered using render_tools.py
            # we don't need to check JSON serialization as @render_tools.render_safely
            # decorator ensures this check so there is no need to re-serialize
            if not issubclass(type(component), UserComponent):
                try:  # check if rendered object is json serializable.
                    json.dumps(rendered_obj)
                except (TypeError, OverflowError) as e:
                    return None
        return rendered_obj
