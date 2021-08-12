class Renderer(object):
    TYPE = None
    
    def _get_mustache(self):
        try:
            from .. import chevron as pt
            return pt
        except ImportError:
            return None

    def render(self,task_datastore):
        return NotImplementedError()

