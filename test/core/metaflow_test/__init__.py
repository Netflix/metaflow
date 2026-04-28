from metaflow import current
from metaflow.cards import get_cards
from metaflow.exception import MetaflowException
from metaflow.plugins.cards.exception import CardNotPresentException


def steps(prio, quals, required=False):
    def wrapper(f):
        f.is_step = True
        f.prio = prio
        f.quals = set(quals)
        f.required = required
        f.tags = []
        return f

    return wrapper


def tag(tagspec, **kwargs):
    def wrapper(f):
        f.tags.append(tagspec)
        return f

    return wrapper


def truncate(var):
    var = str(var)
    if len(var) > 500:
        var = "%s..." % var[:500]
    return var


def retry_until_timeout(cb_fn, *args, timeout=4, **kwargs):
    """
    certain operations in metaflow may not be synchronous and may be running fully asynchronously.
    This creates a problem in writing tests that verify some behaviour at runtime. This function
    is a helper that allows us to wait for a certain amount of time for a callback function to
    return a non-False value.
    """
    import time

    start = time.time()
    while True:
        cb_val = cb_fn(*args, **kwargs)
        if cb_val is not False:
            return cb_val
        if time.time() - start > timeout:
            raise TimeoutError("Timeout waiting for callback to return non-False value")
        time.sleep(1)


def try_to_get_card(id=None, timeout=60):
    """
    Safetly try to get the card object until a timeout value.
    """

    def _get_card(card_id):
        container = get_card_container(id=card_id)
        if container is None:
            return False
        return container[0]

    return retry_until_timeout(_get_card, id, timeout=timeout)


class ResumeFromHere(MetaflowException):
    headline = "Resume requested"

    def __init__(self):
        super(ResumeFromHere, self).__init__(
            "This is not an error. " "Testing resume..."
        )


class TestRetry(MetaflowException):
    headline = "Testing retry"

    def __init__(self):
        super(TestRetry, self).__init__("This is not an error. " "Testing retry...")


def get_card_container(id=None):
    """
    Safetly try to load the card_container object.
    """
    try:
        return get_cards(current.pathspec, id=id)
    except CardNotPresentException:
        return None


def is_resumed():
    return current.origin_run_id is not None


def origin_run_id_for_resume():
    return current.origin_run_id


class FlowDefinition(object):
    """Base class for core integration test flow definitions.

    Each subclass defines step bodies (via @steps/@tag) and a check_results
    method that verifies the completed run.  FlowFormatter combines a
    FlowDefinition with a graph template to produce a runnable FlowSpec.
    """

    PRIORITY = 999999999
    PARAMETERS = {}
    INCLUDE_FILES = {}
    CONFIGS = {}
    CLASS_VARS = {}
    HEADER = ""

    def check_results(self, flow, checker):
        return False


class MetaflowCheck(object):
    def __init__(self, flow, run_id, cli_options=()):
        self._run_id = run_id
        self._cli_options = list(cli_options)

    def get_run(self):
        return None

    @property
    def run_id(self):
        return self._run_id

    @property
    def cli_options(self):
        return self._cli_options

    def assert_artifact(self, step, name, value, fields=None):
        raise NotImplementedError()

    def artifact_dict(self, step, name):
        raise NotImplementedError()

    def assert_log(self, step, logtype, value, exact_match=True):
        raise NotImplementedError()

    def get_card(self, step, task, card_type):
        raise NotImplementedError()

    def get_card_data(self, step, task, card_type, card_id=None):
        """
        returns : (card_present, card_data)
        """
        raise NotImplementedError()

    def list_cards(self, step, task, card_type=None):
        raise NotImplementedError()

    def get_user_tags(self):
        raise NotImplementedError()

    def get_system_tags(self):
        raise NotImplementedError()

    def add_tag(self, tag):
        raise NotImplementedError()

    def add_tags(self, tags):
        raise NotImplementedError()

    def remove_tag(self, tag):
        raise NotImplementedError()

    def remove_tags(self, tags):
        raise NotImplementedError()

    def replace_tag(self, tag_to_remove, tag_to_add):
        raise NotImplementedError()

    def replace_tags(self, tags_to_remove, tags_to_add):
        raise NotImplementedError()


def new_checker(checker_class, flow, run_id, cli_options=()):
    """Create a checker instance.

    checker_class may be the class itself or its name as a string
    ('CliCheck' or 'MetadataCheck').
    """
    from . import cli_check, metadata_check

    _CLASSES = {
        "CliCheck": cli_check.CliCheck,
        "MetadataCheck": metadata_check.MetadataCheck,
    }
    if isinstance(checker_class, str):
        checker_class = _CLASSES[checker_class]
    return checker_class(flow, run_id, cli_options)
