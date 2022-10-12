from datetime import datetime

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


def make_event_body(event_name, event_type, event_data=dict(), capture_time=False):
    timestamp = "TS"
    if capture_time:
        timestamp = int(datetime.utcnow().timestamp())
    return {
        "payload": {
            "event_name": event_name,
            "event_type": event_type,
            "data": event_data,
            "pathspec": current.pathspec,
            "timestamp": timestamp,
        }
    }


def current_flow_name():
    flow_name = current.get("project_flow_name")
    if flow_name is None:
        flow_name = current.flow_name
    return flow_name.lower()


def project_and_branch():
    return (current.get("project_name"), current.get("branch_name"))


# Creates the Argo Event sensor name for a given workflow
def format_sensor_name(flow_name):
    # Argo's internal NATS bus throws an error when attempting to
    # create a persistent queue for the sensor when the sensor name
    # contains '.' which is NATS' topic delimiter.
    updated = flow_name.replace("_", "").replace(".", "-").lower()
    return "mf-" + updated + "-trigger"


def are_events_configured():
    return (
        EVENT_SOURCE_NAME is not None
        and EVENT_SOURCE_URL is not None
        and EVENT_SERVICE_ACCOUNT is not None
    )


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
