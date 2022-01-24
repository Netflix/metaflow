import sys
import os
from metaflow.exception import MetaflowException
from metaflow import current


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


class AssertArtifactFailed(Exception):
    pass


class AssertLogFailed(Exception):
    pass


class AssertCardFailed(Exception):
    pass


class ExpectationFailed(Exception):
    def __init__(self, expected, got):
        super(ExpectationFailed, self).__init__(
            "Expected result: %s, got %s" % (truncate(expected), truncate(got))
        )


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


def is_resumed():
    return current.origin_run_id is not None


def origin_run_id_for_resume():
    return current.origin_run_id


def assert_equals(expected, got):
    if expected != got:
        raise ExpectationFailed(expected, got)


def assert_exception(func, exception):
    try:
        func()
    except exception:
        return
    except Exception as ex:
        raise ExpectationFailed(exception, ex)
    else:
        raise ExpectationFailed(exception, "no exception")


class MetaflowTest(object):
    PRIORITY = 999999999
    PARAMETERS = {}
    INCLUDE_FILES = {}
    CLASS_VARS = {}
    HEADER = ""

    def check_results(self, flow, checker):
        return False


class MetaflowCheck(object):
    def __init__(self, flow):
        pass

    def get_run(self):
        return None

    @property
    def run_id(self):
        return sys.argv[2]

    @property
    def cli_options(self):
        return sys.argv[3:]

    def assert_artifact(self, step, name, value, fields=None):
        raise NotImplementedError()

    def artifact_dict(self, step, name):
        raise NotImplementedError()

    def assert_log(self, step, logtype, value, exact_match=True):
        raise NotImplementedError()

    def get_card(self, step, task, card_type):
        raise NotImplementedError()

    def list_cards(self, step, task, card_type=None):
        raise NotImplementedError()


def new_checker(flow):
    from . import cli_check, metadata_check

    CHECKER = {
        "CliCheck": cli_check.CliCheck,
        "MetadataCheck": metadata_check.MetadataCheck,
    }
    CLASSNAME = sys.argv[1]
    return CHECKER[CLASSNAME](flow)
