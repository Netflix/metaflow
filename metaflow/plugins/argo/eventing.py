import asyncio
from datetime import datetime
import json
import os
import re
import sys
from urllib import parse

from metaflow.current import current
from metaflow.exception import MetaflowException, MetaflowExceptionWrapper
from metaflow.plugins.argo.util import current_flow_name, project_and_branch
import requests


class BadEventNameException(MetaflowException):
    headline = "Bad event name detected"


def send_event(event_name, event_data={}):
    flow_name = current_flow_name()
    (project, branch) = project_and_branch()
    if project is not None:
        event_name = "%s-%s-%s" % (
            project.replace(".", "-").replace("_", ""),
            branch.replace(".", "-"),
            event_name,
        )
    else:
        event_name = event_name.replace(".", "-")
    if re.fullmatch("[a-z0-9\-_\.]+", event_name) is None:
        raise BadEventNameException(
            ("Attempted to send '%s'. " % event_name)
            + "Event names can only contain lowercase characters, digits, '.', '_', and '-'."
        )
    event_source_url = os.getenv("METAFLOW_EVENT_SOURCE")
    if event_source_url.startswith("nats://"):
        # TODO: Make this work
        raise MetaflowException("Sending user events via NATS isn't supported.")
    _send_webhook_event(event_source_url, flow_name, event_name, event_data)
    # if event_source_url is not None and event_source_url.startswith("nats://"):
    #     asyncio.run(_send_nats_event(event_source_url, flow_name, event_name, event_data))
    # else:
    #     _send_webhook_event(event_source_url, flow_name, event_name, event_data)


def _send_webhook_event(event_url, flow_name, event_name, event_data):
    ts = int(datetime.utcnow().timestamp())
    try:
        payload = {
            "payload": {
                "flow_name": flow_name,
                "event_name": event_name,
                "pathspec": current.pathspec,
                "timestamp": ts,
                "data": event_data,
            }
        }
        resp = requests.post(
            event_url, headers={"content-type": "application/json"}, json=payload
        )
        resp.raise_for_status()
    except Exception as e:
        raise MetaflowExceptionWrapper(e)


async def _send_nats_event(flow_name, event_name, event_data):
    try:
        import nats

    except (NameError, ImportError):
        raise MetaflowException(
            "Could not import module 'nats'.\n\nInstall the nats-py "
            "Python package (https://pypi.org/project/nats-py/) first.\n"
            "You can install the module by executing - "
            "%s -m pip install nats-py\n"
            "or equivalent through your favorite Python package manager."
            % sys.executable
        )
    ts = int(datetime.utcnow().timestamp())
    auth_token = os.getenv("NATS_TOKEN")
    parsed = parse.urlparse(EVENT_SOURCE_URL)
    topic = parsed.path.replace("/", "")
    server = "%s://%s" % (parsed.scheme, parsed.netloc)
    payload = {
        "payload": {
            "flow_name": flow_name,
            "event_name": event_name,
            "pathspec": current.pathspec,
            "timestamp": ts,
            "data": event_data,
        }
    }
    body = bytes(json.dumps(payload), "utf-8")
    try:
        conn = await nats.connect(server, token=auth_token)
        await conn.publish(topic, body)
        await conn.drain()
    except Exception as e:
        raise MetaflowExceptionWrapper(e)
