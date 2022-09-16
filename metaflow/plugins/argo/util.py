import base64
import json
import re

from metaflow.metaflow_config import (
    EVENT_SOURCE_URL,
    EVENT_SOURCE_NAME,
    EVENT_SERVICE_ACCOUNT,
)
from metaflow.current import current
from metaflow.exception import MetaflowException


class BadEventConfig(MetaflowException):
    headline = "Event configuration error"


STATUS_TENSES = {"succeeded": "succeeds", "failed": "fails"}


def event_topic():
    if EVENT_SOURCE_URL is None:
        raise BadEventConfig(msg="Required config entry EVENT_SOURCE_URL is missing.")
    chunks = EVENT_SOURCE_URL.split("//")
    if len(chunks) < 2:
        raise BadEventConfig(msg="Unknown EVENT_SOURCE_URL format.")
    url_chunks = chunks[1].split("/")
    if len(url_chunks) < 2:
        raise BadEventConfig(msg="EVENT_SOURCE_URL missing topic.")
    return url_chunks[-1]


def current_flow_name():
    flow_name = current.get("project_flow_name")
    if flow_name is None:
        flow_name = current.flow_name
    return flow_name.replace("_", "-").lower()


def project_and_branch():
    if "project_name" in current:
        project_name = current.project_name
    else:
        return (None, None)
    if "branch_name" in current:
        return (project_name, current.branch_name)
    return (project_name, None)


# Creates the Argo Event sensor name for a given workflow
def format_sensor_name(flow_name):
    updated = flow_name.replace("_", "-").lower()
    return "mf-" + updated + "-after-trigger"


def are_events_configured():
    return (
        EVENT_SOURCE_NAME is not None
        and EVENT_SOURCE_URL is not None
        and EVENT_SERVICE_ACCOUNT is not None
    )


# Translates workflow status into present tense
# Ex: 'succeeded' becomes 'succeeds'
def status_present_tense(status):
    return STATUS_TENSES[status]


def valid_statuses():
    return list(STATUS_TENSES.keys())


def encode_json(data):
    data = bytes(json.dumps(data, separators=(",", ":"), indent=None), "UTF-8")
    encoded_data = base64.b64encode(data)
    return '"%s"' % encoded_data.decode("UTF-8")


def list_to_prose(items, singular, use_quotes=False, plural=None):
    item_count = len(items)
    if plural is None:
        plural = singular + "s"
    item_type = singular
    if item_count == 1:
        if use_quotes:
            template = "'%s'"
        else:
            template = "%s"
        result = template % items[0]
    elif item_count == 2:
        if use_quotes:
            template = "'%s' and '%s'"
        else:
            template = "%s and %s"
        result = template % (items[0], items[1])
        item_type = plural
    elif item_count > 2:
        if use_quotes:
            formatted = ", ".join(
                ["'" + item + "'" for item in items[0 : item_count - 1]]
            )
            result = "%s and '%s'" % (formatted, items[item_count - 1])
        else:
            formatted = ", ".join(items[0 : item_count - 1])
            result = "%s and %s" % (formatted, items[item_count - 1])
            item_type = plural
    else:
        result = ""
    return (item_type, result)
