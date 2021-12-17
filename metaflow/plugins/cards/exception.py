from metaflow.exception import MetaflowException
import traceback
import re

TYPE_CHECK_REGEX = "^[a-zA-Z0-9_]+$"
CARD_ID_PATTERN = re.compile(TYPE_CHECK_REGEX)


class CardClassFoundException(MetaflowException):
    """
    This exception is raised with MetaflowCard class is not present for a particular card type.
    """

    headline = "MetaflowCard not found"

    def __init__(self, card_name):
        exc = traceback.format_exc()
        msg = (
            "MetaflowCard named %s not found. Check the `type` "
            "attribute in @card" % (card_name)
        )
        super(CardClassFoundException, self).__init__(msg)


class TypeRequiredException(MetaflowException):

    headline = "Card type missing exception"

    def __init__(self):
        msg = "if IDENTIFIER is a pathspec than --type is required"
        super().__init__(msg=msg)


class CardNotPresentException(MetaflowException):
    """
    This exception is raised with a card is not present in the datastore.
    """

    headline = "Card not found in datastore"

    def __init__(
        self,
        flow_name,
        run_id,
        step_name,
        card_type=None,
        card_hash=None,
    ):
        idx_msg = ""
        hash_msg = ""
        msg = ""

        if card_hash is not None:
            hash_msg = " and hash %s" % card_hash
        if card_type is not None:
            msg = "Card of type %s %s %s not present for path-spec" " %s/%s/%s" % (
                card_type,
                idx_msg,
                hash_msg,
                flow_name,
                run_id,
                step_name,
            )
        else:
            msg = "Card not present for path-spec" " %s/%s/%s %s %s" % (
                flow_name,
                run_id,
                step_name,
                idx_msg,
                hash_msg,
            )

        super(CardNotPresentException, self).__init__(msg)


class IncorrectCardArgsException(MetaflowException):

    headline = "Incorrect arguements to @card decorator"

    def __init__(self, card_type, args):
        msg = "Card of type %s cannot support arguements" " %s" % (card_type, args)
        super(IncorrectCardArgsException, self).__init__(msg)


class UnrenderableCardException(MetaflowException):

    headline = "Unable to render @card"

    def __init__(self, card_type, args):
        msg = (
            "Card of type %s is unable to be rendered with arguements %s.\nStack trace : "
            " %s" % (card_type, args, traceback.format_exc())
        )
        super(UnrenderableCardException, self).__init__(msg)


class UnresolvableDatastoreException(MetaflowException):

    headline = "Cannot resolve datastore type from `Task.metadata`"

    def __init__(self, task):
        msg = (
            "Cannot resolve the metadata `ds-type` from task with pathspec : %s "
            % task.pathspec
        )
        super(UnresolvableDatastoreException, self).__init__(msg)


class IncorrectArguementException(MetaflowException):
    headline = (
        "`get_cards` function requires a `Task` object or pathspec as an argument"
    )

    def __init__(self, obj_type):
        msg = (
            "`get_cards` function requires a `Task` object or pathspec as an argument. `task` argument cannot be of type %s."
            % str(obj_type)
        )
        super().__init__(msg=msg, lineno=None)


class IncorrectPathspecException(MetaflowException):
    headline = "Pathspec is required of form `flowname/runid/stepname/taskid`"

    def __init__(self, pthspec):
        msg = (
            "Pathspec %s is invalid. Pathspec is required of form `flowname/runid/stepname/taskid`"
            % pthspec
        )
        super().__init__(msg=msg, lineno=None)
