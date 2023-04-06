import os
import random

import requests
import time

from distutils.version import LooseVersion

from metaflow.exception import (
    MetaflowException,
    MetaflowTaggingError,
    MetaflowInternalError,
)
from metaflow.metaflow_config import (
    SERVICE_RETRY_COUNT,
    SERVICE_HEADERS,
    SERVICE_URL,
)
from metaflow.metadata import MetadataProvider
from metaflow.metadata.heartbeat import HB_URL_KEY
from metaflow.sidecar import Message, MessageTypes, Sidecar


# Define message enums
class HeartbeatTypes(object):
    RUN = 1
    TASK = 2


class ServiceException(MetaflowException):
    headline = "Metaflow service error"

    def __init__(self, msg, http_code=None, body=None):
        self.http_code = None if http_code is None else int(http_code)
        self.response = body
        super(ServiceException, self).__init__(msg)


class ServiceMetadataProvider(MetadataProvider):
    TYPE = "service"

    _supports_attempt_gets = None
    _supports_tag_mutation = None

    def __init__(self, environment, flow, event_logger, monitor):
        super(ServiceMetadataProvider, self).__init__(
            environment, flow, event_logger, monitor
        )
        self.url_task_template = os.path.join(
            SERVICE_URL,
            "flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/heartbeat",
        )
        self.url_run_template = os.path.join(
            SERVICE_URL, "flows/{flow_id}/runs/{run_number}/heartbeat"
        )
        self.sidecar = None

    @classmethod
    def compute_info(cls, val):
        v = val.rstrip("/")
        try:
            resp = requests.get(os.path.join(v, "ping"), headers=SERVICE_HEADERS)
            resp.raise_for_status()
        except:  # noqa E722
            raise ValueError("Metaflow service [%s] unreachable." % v)
        return v

    @classmethod
    def default_info(cls):
        return SERVICE_URL

    def version(self):
        return self._version(self._monitor)

    def new_run_id(self, tags=None, sys_tags=None):
        v, _ = self._new_run(tags=tags, sys_tags=sys_tags)
        return v

    def register_run_id(self, run_id, tags=None, sys_tags=None):
        try:
            # don't try to register an integer ID which was obtained
            # from the metadata service in the first place
            int(run_id)
            return False
        except ValueError:
            _, did_create = self._new_run(run_id, tags=tags, sys_tags=sys_tags)
            return did_create

    def new_task_id(self, run_id, step_name, tags=None, sys_tags=None):
        v, _ = self._new_task(run_id, step_name, tags=tags, sys_tags=sys_tags)
        return v

    def register_task_id(
        self, run_id, step_name, task_id, attempt=0, tags=None, sys_tags=None
    ):
        try:
            # don't try to register an integer ID which was obtained
            # from the metadata service in the first place
            int(task_id)
        except ValueError:
            _, did_create = self._new_task(
                run_id,
                step_name,
                task_id=task_id,
                attempt=attempt,
                tags=tags,
                sys_tags=sys_tags,
            )
            return did_create
        else:
            self._register_system_metadata(run_id, step_name, task_id, attempt)
            return False

    def _start_heartbeat(
        self, heartbeat_type, flow_id, run_id, step_name=None, task_id=None
    ):
        if self._already_started():
            # A single ServiceMetadataProvider instance can not start
            # multiple heartbeat side cars of any type/combination. Either a
            # single run heartbeat or a single task heartbeat can be started
            raise Exception("heartbeat already started")
        # create init message
        payload = {}
        if heartbeat_type == HeartbeatTypes.TASK:
            # create task heartbeat
            data = {
                "flow_id": flow_id,
                "run_number": run_id,
                "step_name": step_name,
                "task_id": task_id,
            }
            payload[HB_URL_KEY] = self.url_task_template.format(**data)
        elif heartbeat_type == HeartbeatTypes.RUN:
            # create run heartbeat
            data = {"flow_id": flow_id, "run_number": run_id}

            payload[HB_URL_KEY] = self.url_run_template.format(**data)
        else:
            raise Exception("invalid heartbeat type")
        service_version = self.version()
        payload["service_version"] = service_version
        # start sidecar
        if service_version is None or LooseVersion(service_version) < LooseVersion(
            "2.0.4"
        ):
            # if old version of the service is running
            # then avoid running real heartbeat sidecar process
            self.sidecar = Sidecar("none")
        else:
            self.sidecar = Sidecar("heartbeat")
        self.sidecar.start()
        self.sidecar.send(Message(MessageTypes.BEST_EFFORT, payload))

    def start_run_heartbeat(self, flow_id, run_id):
        self._start_heartbeat(HeartbeatTypes.RUN, flow_id, run_id)

    def start_task_heartbeat(self, flow_id, run_id, step_name, task_id):
        self._start_heartbeat(HeartbeatTypes.TASK, flow_id, run_id, step_name, task_id)

    def _already_started(self):
        return self.sidecar is not None

    def stop_heartbeat(self):
        self.sidecar.terminate()

    def register_data_artifacts(
        self, run_id, step_name, task_id, attempt_id, artifacts
    ):
        url = ServiceMetadataProvider._obj_path(
            self._flow_name, run_id, step_name, task_id
        )
        url += "/artifact"
        data = self._artifacts_to_json(
            run_id, step_name, task_id, attempt_id, artifacts
        )
        self._request(self._monitor, url, "POST", data)

    def register_metadata(self, run_id, step_name, task_id, metadata):
        url = ServiceMetadataProvider._obj_path(
            self._flow_name, run_id, step_name, task_id
        )
        url += "/metadata"
        data = self._metadata_to_json(run_id, step_name, task_id, metadata)
        self._request(self._monitor, url, "POST", data)

    @classmethod
    def _mutate_user_tags_for_run(
        cls, flow_id, run_id, tags_to_add=None, tags_to_remove=None
    ):
        min_service_version_with_tag_mutation = "2.3.0"
        if cls._supports_tag_mutation is None:
            version = cls._version(None)
            cls._supports_tag_mutation = version is not None and LooseVersion(
                version
            ) >= LooseVersion(min_service_version_with_tag_mutation)
        if not cls._supports_tag_mutation:
            raise ServiceException(
                "Adding or removing tags on a run requires the Metaflow service to be "
                "at least version %s. Please upgrade your service."
                % (min_service_version_with_tag_mutation,)
            )

        url = ServiceMetadataProvider._obj_path(flow_id, run_id) + "/tag/mutate"
        tag_mutation_data = {
            # mutate_user_tags_for_run() should have already ensured that this is a list, so let's be tolerant here
            "tags_to_add": list(tags_to_add or []),
            "tags_to_remove": list(tags_to_remove or []),
        }
        tries = 1
        status_codes_seen = set()
        # try up to 10 times, with a gentle exponential backoff (1.4-1.6x)
        while True:
            resp, _ = cls._request(
                None, url, "PATCH", data=tag_mutation_data, return_raw_resp=True
            )
            status_codes_seen.add(resp.status_code)
            # happy path
            if resp.status_code < 300:
                return frozenset(resp.json()["tags"])
            # definitely NOT retriable
            if resp.status_code in (400, 422):
                raise MetaflowTaggingError("Metadata service says: %s" % (resp.text,))
            # if we get here, mutation failure is possibly retriable
            if tries >= 10:
                # if we ever received 409 on any of our attempts, report "conflicting updates" blurb to user
                if 409 in status_codes_seen:
                    raise MetaflowTaggingError(
                        "Tagging failed due to too many conflicting updates from other processes"
                    )
                # No 409's seen... raise a more generic error
                raise MetaflowTaggingError("Tagging failed after %d tries" % tries)
            time.sleep(0.3 * random.uniform(1.4, 1.6) ** tries)
            tries += 1

    @classmethod
    def _get_object_internal(
        cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args
    ):
        if attempt is not None:
            if cls._supports_attempt_gets is None:
                version = cls._version(None)
                cls._supports_attempt_gets = version is not None and LooseVersion(
                    version
                ) >= LooseVersion("2.0.6")
            if not cls._supports_attempt_gets:
                raise ServiceException(
                    "Getting specific attempts of Tasks or Artifacts requires "
                    "the metaflow service to be at least version 2.0.6. Please "
                    "upgrade your service"
                )

        if sub_type == "self":
            if obj_type == "artifact":
                # Special case with the artifacts; we add the attempt
                url = ServiceMetadataProvider._obj_path(
                    *args[:obj_order], attempt=attempt
                )
            else:
                url = ServiceMetadataProvider._obj_path(*args[:obj_order])
            try:
                v, _ = cls._request(None, url, "GET")
                return MetadataProvider._apply_filter([v], filters)[0]
            except ServiceException as ex:
                if ex.http_code == 404:
                    return None
                raise

        # For the other types, we locate all the objects we need to find and return them
        if obj_type != "root":
            url = ServiceMetadataProvider._obj_path(*args[:obj_order])
        else:
            url = ""
        if sub_type == "metadata":
            url += "/metadata"
        elif sub_type == "artifact" and obj_type == "task" and attempt is not None:
            url += "/attempt/%s/artifacts" % attempt
        else:
            url += "/%ss" % sub_type
        try:
            v, _ = cls._request(None, url, "GET")
            return MetadataProvider._apply_filter(v, filters)
        except ServiceException as ex:
            if ex.http_code == 404:
                return None
            raise

    def _new_run(self, run_id=None, tags=None, sys_tags=None):
        # first ensure that the flow exists
        self._get_or_create("flow")
        run, did_create = self._get_or_create(
            "run", run_id, tags=tags, sys_tags=sys_tags
        )
        return str(run["run_number"]), did_create

    def _new_task(
        self, run_id, step_name, task_id=None, attempt=0, tags=None, sys_tags=None
    ):
        # first ensure that the step exists
        self._get_or_create("step", run_id, step_name)
        task, did_create = self._get_or_create(
            "task", run_id, step_name, task_id, tags=tags, sys_tags=sys_tags
        )
        if did_create:
            self._register_system_metadata(run_id, step_name, task["task_id"], attempt)
        return task["task_id"], did_create

    @staticmethod
    def _obj_path(
        flow_name,
        run_id=None,
        step_name=None,
        task_id=None,
        artifact_name=None,
        attempt=None,
    ):
        object_path = "/flows/%s" % flow_name
        if run_id is not None:
            object_path += "/runs/%s" % run_id
        if step_name is not None:
            object_path += "/steps/%s" % step_name
        if task_id is not None:
            object_path += "/tasks/%s" % task_id
        if artifact_name is not None:
            object_path += "/artifacts/%s" % artifact_name
        if attempt is not None:
            object_path += "/attempt/%s" % attempt
        return object_path

    @staticmethod
    def _create_path(obj_type, flow_name, run_id=None, step_name=None):
        create_path = "/flows/%s" % flow_name
        if obj_type == "flow":
            return create_path
        if obj_type == "run":
            return create_path + "/run"
        create_path += "/runs/%s/steps/%s" % (run_id, step_name)
        if obj_type == "step":
            return create_path + "/step"
        return create_path + "/task"

    def _get_or_create(
        self,
        obj_type,
        run_id=None,
        step_name=None,
        task_id=None,
        tags=None,
        sys_tags=None,
    ):

        if tags is None:
            tags = set()
        if sys_tags is None:
            sys_tags = set()

        def create_object():
            data = self._object_to_json(
                obj_type,
                run_id,
                step_name,
                task_id,
                self.sticky_tags.union(tags),
                self.sticky_sys_tags.union(sys_tags),
            )
            return self._request(
                self._monitor, create_path, "POST", data=data, retry_409_path=obj_path
            )

        always_create = False
        obj_path = self._obj_path(self._flow_name, run_id, step_name, task_id)
        create_path = self._create_path(obj_type, self._flow_name, run_id, step_name)
        if obj_type == "run" and run_id is None:
            always_create = True
        elif obj_type == "task" and task_id is None:
            always_create = True

        if always_create:
            return create_object()

        try:
            return self._request(self._monitor, obj_path, "GET")
        except ServiceException as ex:
            if ex.http_code == 404:
                return create_object()
            else:
                raise

    # TODO _request() needs a more deliberate refactor at some point, it looks quite overgrown.
    @classmethod
    def _request(
        cls,
        monitor,
        path,
        method,
        data=None,
        retry_409_path=None,
        return_raw_resp=False,
    ):
        if cls.INFO is None:
            raise MetaflowException(
                "Missing Metaflow Service URL. "
                "Specify with METAFLOW_SERVICE_URL environment variable"
            )
        supported_methods = ("GET", "PATCH", "POST")
        if method not in supported_methods:
            raise MetaflowException(
                "Only these methods are supported: %s, but got %s"
                % (supported_methods, method)
            )
        url = os.path.join(cls.INFO, path.lstrip("/"))
        for i in range(SERVICE_RETRY_COUNT):
            try:
                if method == "GET":
                    if monitor:
                        with monitor.measure("metaflow.service_metadata.get"):
                            resp = requests.get(url, headers=SERVICE_HEADERS)
                    else:
                        resp = requests.get(url, headers=SERVICE_HEADERS)
                elif method == "POST":
                    if monitor:
                        with monitor.measure("metaflow.service_metadata.post"):
                            resp = requests.post(
                                url, headers=SERVICE_HEADERS, json=data
                            )
                    else:
                        resp = requests.post(url, headers=SERVICE_HEADERS, json=data)
                elif method == "PATCH":
                    if monitor:
                        with monitor.measure("metaflow.service_metadata.patch"):
                            resp = requests.patch(
                                url, headers=SERVICE_HEADERS, json=data
                            )
                    else:
                        resp = requests.patch(url, headers=SERVICE_HEADERS, json=data)
                else:
                    raise MetaflowInternalError("Unexpected HTTP method %s" % (method,))
            except MetaflowInternalError:
                raise
            except:  # noqa E722
                if monitor:
                    with monitor.count("metaflow.service_metadata.failed_request"):
                        if i == SERVICE_RETRY_COUNT - 1:
                            raise
                else:
                    if i == SERVICE_RETRY_COUNT - 1:
                        raise
                resp = None
            else:
                if return_raw_resp:
                    return resp, True
                if resp.status_code < 300:
                    return resp.json(), True
                elif resp.status_code == 409 and data is not None:
                    # a special case: the post fails due to a conflict
                    # this could occur when we missed a success response
                    # from the first POST request but the request
                    # actually went though, so a subsequent POST
                    # returns 409 (conflict) or we end up with a
                    # conflict while running on AWS Step Functions
                    # instead of retrying the post we retry with a get since
                    # the record is guaranteed to exist
                    if retry_409_path:
                        v, _ = cls._request(monitor, retry_409_path, "GET")
                        return v, False
                    else:
                        return None, False
                elif resp.status_code != 503:
                    raise ServiceException(
                        "Metadata request (%s) failed (code %s): %s"
                        % (path, resp.status_code, resp.text),
                        resp.status_code,
                        resp.text,
                    )
            time.sleep(2**i)

        if resp:
            raise ServiceException(
                "Metadata request (%s) failed (code %s): %s"
                % (path, resp.status_code, resp.text),
                resp.status_code,
                resp.text,
            )
        else:
            raise ServiceException("Metadata request (%s) failed" % path)

    @classmethod
    def _version(cls, monitor):
        if cls.INFO is None:
            raise MetaflowException(
                "Missing Metaflow Service URL. "
                "Specify with METAFLOW_SERVICE_URL environment variable"
            )
        path = "ping"
        url = os.path.join(cls.INFO, path)
        for i in range(SERVICE_RETRY_COUNT):
            try:
                if monitor:
                    with monitor.measure("metaflow.service_metadata.get"):
                        resp = requests.get(url, headers=SERVICE_HEADERS)
                else:
                    resp = requests.get(url, headers=SERVICE_HEADERS)
            except:
                if monitor:
                    with monitor.count("metaflow.service_metadata.failed_request"):
                        if i == SERVICE_RETRY_COUNT - 1:
                            raise
                else:
                    if i == SERVICE_RETRY_COUNT - 1:
                        raise
                resp = None
            else:
                if resp.status_code < 300:
                    return resp.headers.get("METADATA_SERVICE_VERSION", None)
                elif resp.status_code != 503:
                    raise ServiceException(
                        "Metadata request (%s) failed"
                        " (code %s): %s" % (url, resp.status_code, resp.text),
                        resp.status_code,
                        resp.text,
                    )
            time.sleep(2**i)
        if resp:
            raise ServiceException(
                "Metadata request (%s) failed (code %s): %s"
                % (url, resp.status_code, resp.text),
                resp.status_code,
                resp.text,
            )
        else:
            raise ServiceException("Metadata request (%s) failed" % url)
