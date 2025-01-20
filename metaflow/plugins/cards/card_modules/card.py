from typing import TYPE_CHECKING
import uuid

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
    JSON-encodable data that is passed to the instance, parametrizing the card. The subclass
    may override the constructor to capture and process the options.

    The subclass needs to implement a `render(task)` method that produces the card
    contents in HTML, given the finished task that is represented by a `Task` object.

    Attributes
    ----------
    type : str
        Card type string. Note that this should be a globally unique name, similar to a
        Python package name, to avoid name clashes between different custom cards.

    Parameters
    ----------
    options : Dict
        JSON-encodable dictionary containing user-definable options for the class.
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

    def __init__(self, options={}, components=[], graph=None, flow=None):
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

    # FIXME document
    def render_runtime(self, task, data):
        raise NotImplementedError()

    # FIXME document
    def refresh(self, task, data):
        raise NotImplementedError()

    # FIXME document
    def reload_content_token(self, task, data):
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
        #FIXME document
        """
        raise NotImplementedError()

    def render(self):
        """
        `render` returns a string or dictionary. This class can be called on the client side to dynamically add components to the `MetaflowCard`
        """
        raise NotImplementedError()


def create_component_id(component):
    uuid_bit = "".join(uuid.uuid4().hex.split("-"))[:6]
    return type(component).__name__.lower() + "_" + uuid_bit


def with_default_component_id(func):
    def ret_func(self, *args, **kwargs):
        if self.component_id is None:
            self.component_id = create_component_id(self)
        return func(self, *args, **kwargs)

    return ret_func
