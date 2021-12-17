class MetaflowCard(object):
    type = None

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
        return NotImplementedError()


class MetaflowCardComponent(object):
    def render(self):
        raise NotImplementedError()
