
from metaflow.plugins.card_modules.card import MetaflowCard

class MockCard(MetaflowCard):
    type='mock_card'

    def render(self, task):
        return "This is a mock card for Pathspec %s" % task.pathspec

class ErrorCard(MetaflowCard):
    type='error_card'
    
    # the render function will raise Exception
    def render(self, task):
        raise Exception("Unknown Things Happened")


class TimeoutCard(MetaflowCard):
    type='timeout_card'
    
    def __init__(self,timeout=10):
        super().__init__()
        self._timeout =timeout 

    # the render function will raise Exception
    def render(self, task):
        import time 
        time.sleep(self._timeout)
        return "TimeoutCard finished timeout of %s successfully" % self._timeout

        
CARDS = [
    ErrorCard,
    TimeoutCard,
    MockCard
]