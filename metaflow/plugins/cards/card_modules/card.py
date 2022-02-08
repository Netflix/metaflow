class MetaflowCard(object):
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

    def render(self, task):
        """
        `render` returns a string.
        """
        return NotImplementedError()


class MetaflowCardComponent(object):
    def render(self):
        """
        `render` returns a string or dictionary. This class can be called on the client side to dynamically add components to the `MetaflowCard`
        """
        raise NotImplementedError()
