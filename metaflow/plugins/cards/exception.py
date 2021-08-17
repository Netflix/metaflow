from metaflow.exception import MetaflowException
import traceback

class CardNotFoundException(MetaflowException):
    headline = 'Card not found'
    
    def __init__(self, card_name):
        exc = traceback.format_exc()
        msg = "Card named %s not found. Check the `type` "\
                "attribute in @card" % (card_name)
        super(CardNotFoundException, self).__init__(msg)

