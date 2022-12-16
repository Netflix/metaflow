import asyncio
from datetime import datetime
from time import strptime
import json
import os
import re
import sys
from urllib import parse

from metaflow.metaflow_config import EVENT_SOURCE_URL
from metaflow.plugins.project_decorator import apply_project_namespacing
from metaflow.current import current
from metaflow.exception import MetaflowException, MetaflowExceptionWrapper
from metaflow.plugins.argo.util import (
    current_flow_name,
    project_and_branch,
)
import requests

BAD_RESET_AFTER_MSG = """reset must be either a float, int, or HH:MM time.

Examples
--------
@trigger_on_finish(flows=["FirstFlow", "SecondFlow"], reset="23:59")

@trigger_on_finish(flows=["FirstFlow", "SecondFlow"], reset="4")
"""


class BadResetException(MetaflowException):
    headline = "Bad reset time detected"


class BadEventNameException(MetaflowException):
    headline = "Bad event name detected"


def make_event_body(event_name, event_type, event_data=dict()):
    event_name_field = re.sub("\.|\-", "_", event_name)
    return {
        "payload": {
            event_name_field: True,
            "event_name": event_name,
            "event_type": event_type,
            "data": event_data,
            "pathspec": current.pathspec,
            "timestamp": int(datetime.utcnow().timestamp()),
        }
    }


def send_event(event_name, event_data={}, use_project=False):
    flow_name = current_flow_name()
    if use_project:
        (project, branch) = project_and_branch()
        if project is not None:
            event_name = "%s.%s.%s" % (
                project,
                branch,
                event_name,
            )
    if re.fullmatch("[a-z0-9\-_\.]+", event_name) is None:
        raise BadEventNameException(
            ("Attempted to send '%s'. " % event_name)
            + "Event names can only contain lowercase characters, digits, '.', '_', and '-'."
        )
    body = make_event_body(event_name, "metaflow_user", event_data=event_data)

    event_source_url = os.getenv("METAFLOW_EVENT_SOURCE")
    if event_source_url.startswith("nats://"):
        asyncio.run(_send_nats_event(event_source_url, body))
    else:
        _send_webhook_event(event_source_url, body)


def _send_webhook_event(event_url, body):
    try:
        resp = requests.post(
            event_url, headers={"content-type": "application/json"}, json=body
        )
        resp.raise_for_status()
    except Exception as e:
        raise MetaflowExceptionWrapper(e)


async def _send_nats_event(event_url, body):
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
    auth_token = os.getenv("NATS_TOKEN")
    parsed = parse.urlparse(event_url)
    topic = parsed.path.replace("/", "")
    server = "%s://%s" % (parsed.scheme, parsed.netloc)
    raw_body = bytes(json.dumps(body), "utf-8")
    try:
        conn = await nats.connect(server, token=auth_token)
        await conn.publish(topic, raw_body)
        await conn.drain()
    except Exception as e:
        raise MetaflowExceptionWrapper(e)


class TriggerSet:
    def __init__(self, project, branch, reset):
        project_decorator = None
        self._project = project
        self._branch = branch
        self._parse_reset(reset)
        self.triggers = []

    def _parse_reset(self, reset):
        parsed_time = None
        if reset == "" or reset is None:
            self._reset = None
        else:
            formats = [
                "%H:%M",  # 24 hour time (13:05)
                "%I:%M%p",  # 12 hour time (1:05pm)
                "%I:%M p",  # 12 hour time (1:05 pm)
            ]
            if reset.find(":") > -1:
                parsed = False
                for f in formats:
                    try:
                        self._reset = strptime(reset, f)
                        parsed = True
                        break
                    except ValueError:
                        continue
                if not parsed:
                    raise BadResetException(msg=BAD_RESET_AFTER_MSG)
            else:
                try:
                    self._reset = int(reset)
                except ValueError:
                    try:
                        self._reset = float(reset)
                    except ValueError:
                        raise BadResetException(msg=BAD_RESET_AFTER_MSG)

    @property
    def reset(self):
        return self._reset

    def append(self, trigger_info):
        trigger_info.add_namespacing(self._project, self._branch)
        self.triggers.append(trigger_info)

    def add_namespacing(self, project, branch):
        self._project = project
        self._branch = branch
        for t in self.triggers:
            t.add_namespacing(self._project, self._branch)

    def is_empty(self):
        return len(self.triggers) == 0

    def __len__(self):
        return len(self.triggers)


class TriggerInfo:

    LIFECYCLE_EVENT = 0
    USER_EVENT = 1

    def __init__(self, type):
        self.type = type
        self._project = None
        self._branch = None
        self._name = None
        self.status = None
        self.mappings = {}

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        self._name = new_name

    @property
    def formatted_name(self):
        if self.type == TriggerInfo.LIFECYCLE_EVENT:
            if self._project is not None:
                formatted = apply_project_namespacing(
                    self._name, self._project, self._branch
                )
                chunks = formatted.split(".")
                chunks[0] = self._project
                formatted = ".".join(chunks)
            else:
                formatted = self._name.lower()
        else:
            formatted = self._name.lower()
        return re.sub("\.|\-", "_", formatted)

    def add_namespacing(self, project, branch):
        self._project = project
        self._branch = branch

    def has_mappings(self):
        return self.mappings is not None and self._name in self.mappings

    def __str__(self):
        data = {
            "type": self.type,
            "name": self._formatted_name,
            "original_name": self._name,
            "status": self.status,
            "mappings": len(self.mappings),
        }
        return json.dumps(data, indent=4)
