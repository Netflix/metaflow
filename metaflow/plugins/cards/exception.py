from metaflow.exception import MetaflowException
import traceback

class CardNotFoundException(MetaflowException):
    headline = 'Card not found'
    
    def __init__(self, card_name):
        exc = traceback.format_exc()
        msg = "Card named %s not found. Check the `type` "\
                "attribute in @card" % (card_name)
        super(CardNotFoundException, self).__init__(msg)



class CardNotPresentException(MetaflowException):
    
    headline = 'Card not found'

    def __init__(self,flow_name, run_id,step_name,card_name,):
        msg = 'Card of type %s not present for path-spec'\
            ' %s/%s/%s'%(card_name,flow_name,run_id,step_name)
        super(CardNotFoundException, self).__init__(msg)

