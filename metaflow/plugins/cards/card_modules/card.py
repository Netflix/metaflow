from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from metaflow.client import Task


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

    type = None

    ALLOW_USER_COMPONENTS = False

    scope = "task"  # can be task | run

    def __init__(self, options={}, components=[], graph=None):
        pass

    def _get_mustache(self):
        try:
            from . import chevron as pt

            return pt
        except ImportError:
            return None

    def render(self, task) -> str:
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


class MetaflowCardComponent(object):
    def render(self):
        """
        `render` returns a string or dictionary. This class can be called on the client side to dynamically add components to the `MetaflowCard`
        """
        raise NotImplementedError()
