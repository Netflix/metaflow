from .card_modules import MetaflowCardComponent
from .card_modules.basic import ErrorComponent
import random
import string


class SerializationErrorComponent(ErrorComponent):
    def __init__(self, component_name, error_message):
        headline = "Component %s [RENDER FAIL]" % component_name
        super().__init__(headline, error_message)


def get_card_class(card_type):
    from metaflow.plugins import CARDS

    filtered_cards = [card for card in CARDS if card.type == card_type]
    if len(filtered_cards) == 0:
        return None
    return filtered_cards[0]


class CardComponentCollector:
    """
    This class helps collect `MetaflowCardComponent`s during runtime execution

    ### Usage with `current`
    `current.cards` is of type `CardComponentCollector`

    ### Main Usage TLDR
    - current.cards.append customizes the default editable card.
    - Only one card can be default editable in a step.
    - The card class must have `ALLOW_USER_COMPONENTS=True` to be considered default editable.
        - Classes with `ALLOW_USER_COMPONENTS=False` are never default editable.
    - The user can specify an id argument to a card, in which case the card is editable through `current.cards[id].append`.
        - A card with an id can be also default editable, if there are no other cards that are eligible to be default editable.
    - If multiple default-editable cards exist but only one card doesn’t have an id, the card without an id is considered to be default editable.
    - If we can’t resolve a single default editable card through the above rules, `current.cards`.append calls show a warning but the call doesn’t fail.
    - A card that is not default editable can be still edited through its id or by looking it up by its type, e.g. `current.cards.get(type=’pytorch’)`.


    """

    def __init__(self, logger=None):
        # self._cards is a dict with uuid to card_component mappings.
        self._cards = {}
        self._card_meta = []
        self._card_id_map = {}  # card_id to uuid map
        self._logger = logger
        # `self._default_editable_card` holds the uuid of the card that is default editable.
        self._default_editable_card = None

    @staticmethod
    def create_uuid():
        return "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(6)
        )

    def _log(self, *args, **kwargs):
        if self._logger:
            self._logger(*args, **kwargs)

    def _add_card(self, card_type, card_id, editable=False):
        card_uuid = self.create_uuid()
        card_metadata = dict(
            type=card_type, uuid=card_uuid, card_id=card_id, editable=editable
        )

        self._card_meta.append(card_metadata)
        self._cards[card_uuid] = []
        return card_metadata

    def _warning(self, message):
        msg = "[@card WARNING] %s" % message
        self._log(msg, timestamp=False, bad=True)

    def _finalize(self):
        # Perform some sanitization checks
        # - Figure if there can be a `default_editable_card`
        # - Ensure that id's are unique and not None if there are more than one editable cards.
        all_card_meta = self._card_meta

        editable_cards_meta = [c for c in all_card_meta if c["editable"]]

        if len(editable_cards_meta) == 0:
            return

        id_set = set()
        for card_meta in editable_cards_meta:
            if card_meta["card_id"] is not None:
                id_set.add(card_meta["card_id"])
                self._card_id_map[card_meta["card_id"]] = card_meta["uuid"]

        # If there is only one editable card then it is becomes `_default_editable_card`
        if len(editable_cards_meta) == 1:
            self._default_editable_card = editable_cards_meta[0]["uuid"]
            return

        # Segregate cards which have id as none and those which dont.
        not_none_id_cards = [c for c in editable_cards_meta if c["card_id"] is not None]
        none_id_cards = [c for c in editable_cards_meta if c["card_id"] is None]

        # If there is only 1 card with id set to None then we can use that as the default card.
        if len(none_id_cards) == 1:
            self._default_editable_card = none_id_cards[0]["uuid"]
        else:
            # throw a warning that more than one card which is editable has no id set to it.
            self._warning(
                (
                    "More than one editable card has `id` set to `None`. "
                    "Please set `id` to each card if you wish to disambiguate using `current.cards['my_card_id']. "
                )
            )

        # If set of ids is not equal to total number of cards with ids then warn the user that we cannot disambiguate so `current.cards['my_card_id']`
        if len(not_none_id_cards) != len(id_set):
            non_unique_ids = [
                idx
                for idx in id_set
                if len(list(filter(lambda x: x["card_id"] == idx, not_none_id_cards)))
                > 1
            ]
            nui = ", ".join(non_unique_ids)
            # throw a warning that decorators have non unique Ids
            self._warning(
                (
                    "Multiple `@card` decorator have been annotated with non unique ids : %s. "
                    "Disabling interfaces for card ids : `%s`. "
                    "( Meaning that `current.cards['%s']` will not work )"
                )
                % (nui, nui, non_unique_ids[0])
            )

            # remove the non unique ids from the `_card_id_map`
            for idx in non_unique_ids:
                del self._card_id_map[idx]

    def __getitem__(self, key):
        if key in self._card_id_map:
            card_uuid = self._card_id_map[key]
            return self._cards[card_uuid]

        self._warning(
            "`current.cards['%s']` is not present. Please set the `id` argument in @card with %s to allow."
            % (key, key)
        )
        return []

    def __setitem__(self, key, value):
        if key in self._card_id_map:
            card_uuid = self._card_id_map[key]
            self._cards[card_uuid] = value

        self._warning(
            "`current.cards['%s']` is not present. Please set the `id` argument in @card with %s to allow."
            % (key, key)
        )

    def append(self, component):
        if self._default_editable_card is None:
            self._warning(
                "`append` cannot disambiguate between multiple cards. Please add the `id` argument when adding multiple decorators."
            )
            return
        self._cards[self._default_editable_card].append(component)

    def extend(self, components):
        if self._default_editable_card is None:
            self._warning(
                "`extend` cannot disambiguate between multiple @card decorators. Please add an `id` argument when adding multiple editable cards."
            )
            return

        self._cards[self._default_editable_card].extend(components)

    def _serialize_components(self, card_uuid):
        import traceback

        serialized_components = []
        if card_uuid not in self._cards:
            return []
        for component in self._cards[card_uuid]:
            if not issubclass(type(component), MetaflowCardComponent):
                continue
            try:
                rendered_obj = component.render()
                assert type(rendered_obj) == str or type(rendered_obj) == dict
                serialized_components.append(rendered_obj)
            except AssertionError:
                serialized_components.append(
                    SerializationErrorComponent(
                        component.__class__.__name__,
                        "Component render didn't return a string",
                    ).render()
                )
            except:
                error_str = traceback.format_exc()
                serialized_components.append(
                    SerializationErrorComponent(
                        component.__class__.__name__, error_str
                    ).render()
                )
        return serialized_components
