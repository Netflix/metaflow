from .card_modules import MetaflowCardComponent
from .card_modules.basic import ErrorComponent


class SerializationErrorComponent(ErrorComponent):
    def __init__(self, component_name, error_message):
        headline = "Component %s [RENDER FAIL]" % component_name
        super().__init__(headline, error_message)


class CardComponentCollector:
    """
    This class helps collect `MetaflowCardComponent`s during runtime execution

    ### Usage with `current`
    `current.cards` is of type `CardComponentCollector`

    ```python
    # Add to single card
    current.cards['default'].append(TableComponent())
    # Add to all cards
    current.cards.append(TableComponent())
    ```
    ### Some Gotcha's

    If a decorator has the same type for multiple steps then we need to gracefully exit or
    ignore that redundant card decorator.
    """

    def __init__(self, types=[]):
        self._cards = {_type: [] for _type in types}

    def __getitem__(self, key):
        return self._cards.get(key, None)

    def __setitem__(self, key, value):
        self._cards[key] = value

    def _add_type(self, card_type):
        self._cards[card_type] = []

    def append(self, component):
        for ct in self._cards:
            self._cards[ct].append(component)

    def extend(self, components):
        for ct in self._cards:
            self._cards[ct].extend(components)

    def serialize_components(self, component_type):
        import traceback

        serialized_components = []
        if component_type not in self._cards:
            return []
        for component in self._cards[component_type]:
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
