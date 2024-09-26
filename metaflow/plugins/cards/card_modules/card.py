from typing import TYPE_CHECKING, Union, Dict, Optional, List, Any

if TYPE_CHECKING:
    import metaflow


class MetaflowCard(object):
    """
    Metaflow cards derive from this base class.

    Subclasses of this class are called *card types*. The desired card
    type `T` is defined in the `@card` decorator as `@card(type=T)`.

    After a task with `@card(type=T, options=S)` finishes executing, Metaflow instantiates
    a subclass `C` of `MetaflowCard` that has its `type` attribute set to `T`, i.e. `C.type=T`.
    The constructor is given the options dictionary `S` that contains arbitrary
    JSON-encodable data that is passed to the instance, parameterizing the card. The subclass
    may override the constructor to capture and process the options.

    The subclass needs to implement a `render(task)` method that produces the card
    contents in HTML, given the finished task that is represented by a `Task` object.

    Attributes
    ----------
    type : str
        Card type string. Note that this should be a globally unique name, similar to a
        Python package name, to avoid name clashes between different custom cards.
    """

    # RELOAD_POLICY determines whether UIs should
    # reload intermediate cards produced by render_runtime
    # or whether they can just rely on data updates

    # the UI may keep using the same card
    # until the final card is produced
    RELOAD_POLICY_NEVER = "never"

    # the UI should reload card every time
    # render_runtime() has produced a new card
    RELOAD_POLICY_ALWAYS = "always"

    # derive reload token from data and component
    # content - force reload only when the content
    # changes. The actual policy is card-specific,
    # defined by the method reload_content_token()
    RELOAD_POLICY_ONCHANGE = "onchange"

    # this token will get replaced in the html with a unique
    # string that is used to ensure that data updates and the
    # card content matches
    RELOAD_POLICY_TOKEN = "[METAFLOW_RELOAD_TOKEN]"

    type = None

    ALLOW_USER_COMPONENTS = False
    RUNTIME_UPDATABLE = False
    RELOAD_POLICY = RELOAD_POLICY_NEVER

    scope = "task"  # can be task | run

    # FIXME document runtime_data
    runtime_data = None

    def __init__(
        self,
        options: Optional[Dict[Any, Any]] = None,
        components: Optional[List["MetaflowCardComponent"]] = None,
        graph: Optional[Dict[str, Any]] = None,
    ):
        """
        Card base type.

        Parameters
        ----------
        options : Dict[Any, Any], optional, default None
            Options for the card
        components : List[MetaflowCardComponent], optional, default None
            Initial components for this card. Other components can then be added using
            the `append` call.
        graph : Dict[str, Any], optional, default None
            The graph for the current flow. Each item in the dictionary will be keyed
            by the step's name and the value will contain information about the decorators
            on the step and various other information about the step.
        """
        pass

    def _get_mustache(self):
        try:
            from . import chevron as pt

            return pt
        except ImportError:
            return None

    def render(self, task: "metaflow.Task") -> str:
        """
        Produce custom card contents in HTML.

        Subclasses override this method to customize the card contents.

        Parameters
        ----------
        task : Task
            A `Task` object that allows you to access data from the finished task and tasks
            preceding it.

        Returns
        -------
        str
            Card contents as an HTML string.
        """
        return NotImplementedError()

    def render_runtime(self, task, data):
        """
        Produces a HTML of card contents during runtime of the task.

        Subclasses override this method when users wish to have realtime updates with cards.

        Parameters
        ----------
        task : Task
            A `Task` object that allows you to access data from Task during runtime

        data : Dict
            Data object that is passed to the card during runtime. The dictionary will be of the form:
            ```python
            {
                "user": user_data, # any passed to `current.card.refresh` function
                "components": component_dict, # all rendered REALTIME_UPDATABLE components
                "render_seq": seq,
                # `render_seq` is a counter that is incremented every time `render_runtime` is called.
                # If a metaflow card has a RELOAD_POLICY_ALWAYS set then the reload token will be set to this value
                # so that the card reload on the UI everytime `render_runtime` is called.
                "component_update_ts": 1727369970,
                # `component_update_ts` is the timestamp of the last time the component array was modified.
                # `component_update_ts` can get used by the `reload_content_token` to make decisions on weather to
                # reload the card on the UI when component array has changed.
                "mode": mode,
            }
            ```
        Returns
        -------
        str
            Card contents as an HTML string.
        """
        raise NotImplementedError()

    def refresh(self, task, data):
        """
        Refresh the card contents during runtime of the task.

        This function returns a dictionary that will be passed down to the HTML returned by `render_runtime` function.
        The return value will be passed to the `metaflow_card_update` Javascript function inside the HTML returned by `render_runtime` function.

        Subclasses override this method when users wish to have realtime updates with cards.

        Parameters
        ----------
        task : Task
            A `Task` object that allows you to access data from Task during runtime

        data : Dict
            Data object that is passed to the card during runtime. The dictionary will be of the form:
            ```python
            {
                "user": user_data, # any passed to `current.card.refresh` function
                "components": component_dict, # all rendered REALTIME_UPDATABLE components
                "render_seq": seq,
                # `render_seq` is a counter that is incremented every time `render_runtime` is called.
                # If a metaflow card has a RELOAD_POLICY_ALWAYS set then the reload token will be set to this value
                # so that the card reload on the UI everytime `render_runtime` is called.
                "component_update_ts": 1727369970,
                # `component_update_ts` is the timestamp of the last time the component array was modified.
                # `component_update_ts` can get used by the `reload_content_token` to make decisions on weather to
                # reload the card on the UI when component array has changed.
                "mode": mode,
            }
            ```

        Returns
        -------
        Dict
            Dictionary that will be passed down to the HTML returned by `render_runtime` function.
        """
        raise NotImplementedError()

    def reload_content_token(self, task, data):
        """
        This function will create a token that will be embedded in the HTML of the card.
        Based on the value of this token, the UI will decide whether to refetch the card's HTML or not.
        This function will be called when the `MetaflowCard.RELOAD_POLICY` set to `onchange` (i.e. `MetaflowCard.RELOAD_POLICY_ONCHANGE`)
        The `data` object passed to this function can help in making decisions on whether to reload the card or not.

        Subclasses override this method when users wish control when the card should be reloaded on the UI.

        Parameters
        ----------
        task : Task
            A `Task` object that allows you to access data from Task during runtime

        data : Dict
            Data object that is passed to the card during runtime. The dictionary will be of the form:
            ```python
            {
                "user": user_data, # any passed to `current.card.refresh` function
                "components": component_dict, # all rendered REALTIME_UPDATABLE components
                "render_seq": seq,
                # `render_seq` is a counter that is incremented every time `render_runtime` is called.
                # If a metaflow card has a RELOAD_POLICY_ALWAYS set then the reload token will be set to this value
                # so that the card reload on the UI everytime `render_runtime` is called.
                "component_update_ts": 1727369970,
                # `component_update_ts` is the timestamp of the last time the component array was modified.
                # `component_update_ts` can get used by the `reload_content_token` to make decisions on weather to
                # reload the card on the UI when component array has changed.
                "mode": mode,
            }
            ```

        Returns
        -------
        str
            Token that will be embedded in the HTML of the card.
        """
        return "content-token"


class MetaflowCardComponent(object):

    # Setting REALTIME_UPDATABLE as True will allow metaflow to update the card
    # during Task runtime.
    REALTIME_UPDATABLE = False

    _component_id = None

    _logger = None

    @property
    def component_id(self):
        return self._component_id

    @component_id.setter
    def component_id(self, value):
        if not isinstance(value, str):
            raise TypeError("Component ID must be a string")
        self._component_id = value

    def update(self, *args, **kwargs):
        """
        Helps update the internal state of a component when the component is set to `REALTIME_UPDATABLE=True`
        When components get updated during runtime, the `current.card.refresh` function will ensure that the updated component is passed to the card.
        Subclasses override this method when users wish to update the component during runtime.

        Returns
        -------
        None

        """
        raise NotImplementedError()

    def render(self) -> Union[str, Dict]:
        """
        Converts the component to a string or a dictionary so that it's content can be passed down to the MetaflowCard.

        Returns
        -------
        Union[str, Dict]
            The component content as a string or a dictionary
        """
        raise NotImplementedError()
