import os
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


# Parses a flow name into its constituent parts
# Returns a tuple of (project_name, branch_name, flow_name)
def parse_flow_name(flow_name):
    chunks = flow_name.split(".")
    chunk_count = len(chunks)
    if chunk_count == 1:
        return (None, None, chunks[0])
    elif chunk_count == 3:
        return (chunks[0], chunks[1], chunks[2])
    elif chunk_count == 4:
        return (chunks[0], ".".join((chunks[1], chunks[2])), chunks[3])
    else:
        raise MetaflowException(
            msg="Unknown flow name format. "
            + "Expected 1, 3, or 4 chunks and have %d" % chunk_count
        )


# This is a streamlined version of the logic from project_decorator.format_name()
# Validation has been removed since the only time this function should return the
# default flow name if the project decorator isn't used.
def format_flow_name(flow_name, project_name=None, branch_name=None):
    argo_name = flow_name.replace("_", "-").lower()
    if project_name is None:
        return (flow_name, argo_name)
    project_name = re.sub("_|\-", "", project_name)
    return (
        ".".join((project_name, branch_name, flow_name)),
        ".".join((project_name, branch_name, argo_name)),
    )


# Creates the Argo Event sensor name for a given workflow
def format_sensor_name(flow_name):
    updated = flow_name.replace("_", "-").lower()
    return "mf-" + updated + "-after"


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
