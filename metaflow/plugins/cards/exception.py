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

    def __init__(self, pathspec, card_type=None, card_hash=None, card_id=None):
        main_message = "Card not found for pathspec %s" % pathspec
        if card_id is not None:
            main_message = "Card with id '%s' not found for pathspec %s" % (
                card_id,
                pathspec,
            )

        if card_type is not None:
            main_message = "Card with type '%s' not found for pathspec %s" % (
                card_type,
                pathspec,
            )

        if card_hash is not None:
            main_message = (
                "Card with hash '%s' not found for pathspec %s. When using `--hash` always ensure you have full hash or first five characters of the hash."
                % (card_hash, pathspec)
            )

        super(CardNotPresentException, self).__init__(main_message)


class TaskNotFoundException(MetaflowException):

    headline = "Cannot resolve task for pathspec"

    def __init__(
        self,
        pathspec_query,
        resolved_from,
        run_id=None,
    ):
        message = "Cannot resolve task to find card."
        if resolved_from == "task_pathspec":
            message = "Task pathspec %s not found." % pathspec_query
        elif resolved_from == "step_pathspec":
            message = "Step pathspec %s not found." % pathspec_query
        elif resolved_from == "stepname":
            message = "Step %s not found" % pathspec_query
            if run_id is not None:
                message = "Step %s not found for Run('%s')." % (pathspec_query, run_id)
        super().__init__(msg=message, lineno=None)


class IncorrectCardArgsException(MetaflowException):

    headline = "Incorrect arguments to @card decorator"

    def __init__(self, card_type, args):
        msg = "Card of type %s cannot support arguments" " %s" % (card_type, args)
        super(IncorrectCardArgsException, self).__init__(msg)


class UnrenderableCardException(MetaflowException):

    headline = "Unable to render @card"

    def __init__(self, card_type, args):
        msg = (
            "Card of type %s is unable to be rendered with arguments %s.\nStack trace : "
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
