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

class ErroredComponent(MetaflowCardComponent):

    def __init__(self,component_name,exception) -> None:
        self.component_name = component_name
        self.exception = exception

    def render(self):
        return """
        <div>
            <p>Component %s was not rendered because of Error<p>
            <p>%s <p>
        <div/>
        """ %(self.component_name,self.exception)
        

def add_to_card(past_component_arr,card_components):
    additions = []
    for comp in card_components:
        if issubclass(type(comp),MetaflowCardComponent):
            additions.append(comp)
    past_component_arr.extend(additions)

def serialize_components(past_component_arr):
    import traceback
    serialized_components = []
    for component in past_component_arr:
        try:
            rendered_obj = component.render()
            assert type(rendered_obj) == str
            serialized_components.append(
                rendered_obj
            )
        except AssertionError:
            serialized_components.append(
                ErroredComponent(component.__class__.__name__,"Component render didn't return a string").render()
            )
        except:
            error_str = traceback.format_exc()
            serialized_components.append(
                ErroredComponent(component.__class__.__name__,error_str).render()
            )
    return serialized_components
        
        