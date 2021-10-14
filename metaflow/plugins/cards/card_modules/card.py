class MetaflowCard(object):
    type = None

    scope = 'task' # can be task | run 
    def __init__(self,options={}):
        pass
    
    def _get_mustache(self):
        try:
            from . import chevron as pt
            return pt
        except ImportError:
            return None

    def render(self,task):
        return NotImplementedError()


class MetaflowCardComponent(object):

    def render(self):
        raise NotImplementedError()

def add_to_card(past_component_arr,card_components):
    additions = []
    for comp in card_components:
        if issubclass(type(comp),MetaflowCardComponent):
            additions.append(comp)
    past_component_arr.extend(additions)

def serialize_components(past_component_arr):
    return [component.render() for component in past_component_arr]
        