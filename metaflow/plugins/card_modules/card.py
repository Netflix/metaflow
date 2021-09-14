class MetaflowCard(object):
    name = None

    type = 'task' # can be task | run 
    
    def _get_mustache(self):
        try:
            from . import chevron as pt
            return pt
        except ImportError:
            return None

    def render(self,task):
        return NotImplementedError()

