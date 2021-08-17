from metaflow.exception import MetaflowException
import traceback

class CardNotFoundException(MetaflowException):
    headline = 'Card not found'
    
    def __init__(self, card_name):
        exc = traceback.format_exc()
        msg = f"Card named {card_name} not found. Check the `type` "\
                "attribute in @card"
        super(CardNotFoundException, self).__init__(msg)

