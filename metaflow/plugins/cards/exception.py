from metaflow.exception import MetaflowException
import traceback

class CardClassFoundException(MetaflowException):
    """
    This exception is raised with MetaflowCard class is not present for a particular card type.
    """
    headline = 'MetaflowCard not found'
    
    def __init__(self, card_name):
        exc = traceback.format_exc()
        msg = "MetaflowCard named %s not found. Check the `type` "\
                "attribute in @card" % (card_name)
        super(CardClassFoundException, self).__init__(msg)


class BadCardNameException(MetaflowException):
    headline = 'Unsupportable id in @card'
    
    def __init__(self, card_name):
        exc = traceback.format_exc()
        msg = "Card with id %s is not supported. "\
                "Card ids should follow the pattern : [a-zA-Z0-9_]" % (card_name)
        super(BadCardNameException, self).__init__(msg)

class IdNotFoundException(MetaflowException):

    headline = 'Cannot find card id'

    def __init__(self,card_id):
        msg = 'Cannot find card with id %s in the datastore' % card_id
        super().__init__(msg=msg)


class TypeRequiredException(MetaflowException):

    headline = 'Card type missing exception'

    def __init__(self):
        msg = 'if IDENTIFIER is a pathspec than --type is required'
        super().__init__(msg=msg)

class CardNotPresentException(MetaflowException):
    """
    This exception is raised with a card is not present in the datastore. 
    """
    
    headline = 'Card not found in datastore'

    def __init__(self,flow_name, run_id,step_name,card_type=None,card_id=None,card_index=None):
        idx_msg = ''
        msg =''
        if card_index is None:
            idx_msg = ' and index %s ' % card_index
            
        if card_type is not None:
            msg = 'Card of type %s %snot present for path-spec'\
                ' %s/%s/%s'%(card_type,idx_msg,flow_name,run_id,step_name)
        elif card_id is not None:
            msg = 'Card of id %s %s not present for path-spec'\
                ' %s/%s/%s'%(card_id,idx_msg,flow_name,run_id,step_name)
        
        super(CardNotPresentException, self).__init__(msg)


class IncorrectCardArgsException(MetaflowException):
    
    headline = 'Incorrect arguements to @card decorator'

    def __init__(self,card_type,args):
        msg = 'Card of type %s cannot support arguements'\
            ' %s'%(card_type,args)
        super(IncorrectCardArgsException, self).__init__(msg)


class UnrenderableCardException(MetaflowException):
    
    headline = 'Unable to render @card'

    def __init__(self,card_type,args):
        msg = 'Card of type %s is unable to be rendered with arguements %s.\nStack trace : '\
            ' %s'%(card_type,args,traceback.format_exc())
        super(UnrenderableCardException, self).__init__(msg)

