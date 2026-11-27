import os
import random
import time

import requests

from typing import List
from metaflow.exception import (
    MetaflowException,
    MetaflowInternalError,
    MetaflowTaggingError,
)
from metaflow.metadata_provider import MetadataProvider
from metaflow.metadata_provider.heartbeat import HB_URL_KEY
from metaflow.metaflow_config import (
    SERVICE_HEADERS,
    SERVICE_PAGE_SIZE,
    SERVICE_RETRY_COUNT,
    SERVICE_URL,
)
from metaflow.sidecar import Message, MessageTypes, Sidecar
from urllib.parse import urlencode
from metaflow.util import version_parse


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

    _session = requests.Session()
    _session.mount(
        "http://",
        requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0,  # Handle retries explicitly
            pool_block=False,
        ),
    )
    _session.mount(
        "https://",
        requests.adapters.HTTPAdapter(
            pool_connections=20, pool_maxsize=20, max_retries=0, pool_block=False
        ),
    )

    # Capability gating is keyed by the metadata service URL. metadata("service@...")
    # can repoint a single process at a different service at runtime, so a version
    # learned from one endpoint must never decide capabilities for another.
    _service_version_cache = {}

    _NEXT_CURSOR_HEADER = "X-Next-Cursor"
    # The metadata service refuses a larger _limit, so ask for at most this many.
    _MAX_SERVICE_PAGE_SIZE = 500
    _LIMIT_HEADER = "X-Limit"
    # 2.6.0 introduced cursor pagination and filtering, but its paginated
    # artifact query reduced to the newest row per artifact *name* rather than
    # to the task's latest attempt. 2.6.1 (metaflow-service#497) fixed that, so
    # it is the first release the paginated path can be used against for every
    # collection, artifacts included.
    _MIN_SERVICE_VERSION_WITH_CURSOR_PAGINATION = "2.6.1"

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
        for i in range(SERVICE_RETRY_COUNT):
            try:
                resp = cls._session.get(
                    os.path.join(v, "ping"), headers=SERVICE_HEADERS.copy()
                )
                resp.raise_for_status()
            except:  # noqa E722
                time.sleep(2 ** (i - 1))
            else:
                return v

        raise ValueError("Metaflow service [%s] unreachable." % v)

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
        if service_version is None or version_parse(service_version) < version_parse(
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
        if not cls._service_supports(min_service_version_with_tag_mutation):
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
    def _service_supports(cls, min_version):
        """Whether the currently selected metadata service is at least min_version.

        Returns a boolean only -- each caller owns its own error message. The
        service version is cached per service URL, so one ping serves every
        capability check against that service, and switching services rechecks.
        """
        url = cls.INFO
        if url not in cls._service_version_cache:
            cls._service_version_cache[url] = cls._version(None)
        version = cls._service_version_cache[url]
        return version is not None and version_parse(version) >= version_parse(
            min_version
        )

    @classmethod
    def _service_supports_cursor_pagination(cls):
        return cls._service_supports(cls._MIN_SERVICE_VERSION_WITH_CURSOR_PAGINATION)

    @staticmethod
    def _header_value(headers, name):
        if not headers:
            return None
        value = headers.get(name)
        if value is not None:
            return value
        for key, val in headers.items():
            if str(key).lower() == name.lower():
                return val
        return None

    @classmethod
    def _collection_path(cls, obj_type, obj_order, sub_type, attempt, *args):
        if obj_type != "root":
            url = cls._obj_path(*args[:obj_order])
        else:
            url = ""
        if sub_type == "metadata":
            url += "/metadata"
        elif sub_type == "artifact" and obj_type == "task" and attempt is not None:
            url += "/attempt/%s/artifacts" % attempt
        else:
            url += "/%ss" % sub_type
        return url

    @classmethod
    def _can_paginate_collection(cls, sub_type, attempt):
        # "self" is a single object, not a collection, and the per-attempt
        # artifact endpoint has no cursor pagination. Everything else, artifacts
        # included, is paginated once the service version gate passes (see
        # _MIN_SERVICE_VERSION_WITH_CURSOR_PAGINATION).
        return sub_type != "self" and attempt is None

    @classmethod
    def _listing_query(cls, query_filters, filters, page_size, cursor):
        query = {}
        for key, value in (query_filters or {}).items():
            key = str(key)
            if key in ("_cursor", "_limit"):
                raise ValueError("%s is controlled by the client" % key)
            if isinstance(value, (list, tuple, set, frozenset)):
                value = ",".join(str(v) for v in value)
            else:
                value = str(value)
            query[key] = value

        tag_filters = []
        for value in (filters or {}).values():
            if isinstance(value, (list, tuple, set, frozenset)):
                tag_filters.extend(str(v) for v in value)
            else:
                tag_filters.append(str(value))
        if tag_filters:
            current_tags = query.get("_tags:all")
            if current_tags:
                tag_filters.insert(0, current_tags)
            query["_tags:all"] = ",".join(tag_filters)

        query["_limit"] = page_size
        if cursor is not None:
            query["_cursor"] = cursor
        return query

    @classmethod
    def _legacy_get_collection(
        cls,
        obj_type,
        obj_order,
        sub_type,
        filters,
        attempt,
        *args,
        raise_on_missing=False,
    ):
        url = cls._collection_path(obj_type, obj_order, sub_type, attempt, *args)
        try:
            v, _ = cls._request(None, url, "GET")
            return MetadataProvider._apply_filter(v, filters)
        except ServiceException as ex:
            if ex.http_code == 404:
                if raise_on_missing:
                    raise
                return None
            raise

    @classmethod
    def _iter_paginated_records(
        cls,
        obj_type,
        obj_order,
        sub_type,
        filters,
        attempt,
        *args,
        query_filters=None,
        page_size=None,
        raise_on_missing=False,
    ):
        page_size = SERVICE_PAGE_SIZE if page_size is None else page_size
        if isinstance(page_size, bool) or not isinstance(page_size, int):
            raise TypeError("page_size must be an integer")
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        page_size = min(page_size, cls._MAX_SERVICE_PAGE_SIZE)

        path = cls._collection_path(obj_type, obj_order, sub_type, attempt, *args)
        cursor = None
        seen_cursors = set()
        first_page = True
        query_filters = query_filters or {}
        # Result ordering is implicit: we send no _order query param (none exists
        # yet), so pages arrive in the service's default order, which is
        # newest-first (descending ts_epoch). MetadataProvider.iter_objects
        # applies the same descending ts_epoch sort for legacy/local providers,
        # so ordering stays consistent regardless of which path served the records.
        while True:
            page_query = cls._listing_query(query_filters, filters, page_size, cursor)
            page_path = "%s?%s" % (path, urlencode(page_query, doseq=True))
            try:
                records, headers = cls._request(
                    None, page_path, "GET", return_headers=True
                )
            except ServiceException as ex:
                if ex.http_code == 404:
                    # A missing collection. When the caller needs to tell "not
                    # found" (None) apart from "empty" ([]) -- e.g. get_object --
                    # re-raise no matter which page 404'd, so the whole listing
                    # resolves to None and keeps legacy's atomic full-result-or-
                    # None contract (never a silently truncated partial list).
                    # Streaming callers instead just end the stream here.
                    if raise_on_missing:
                        raise
                    return
                raise
            # Every page is checked, not just the first: during a rolling
            # deployment page 2 can land on an older instance that ignores our
            # cursor and filters and answers with an unpaginated bulk listing.
            if cls._header_value(headers, cls._LIMIT_HEADER) is None:
                if not first_page:
                    # Records have already been yielded, so falling back to the
                    # legacy listing here would duplicate or unfilter the stream.
                    # A loud failure beats a silently wrong listing.
                    raise ServiceException(
                        "Metadata service stopped honoring cursor pagination "
                        "mid-listing; the listing may be incomplete or unfiltered."
                    )
                if query_filters:
                    raise ServiceException(
                        "Filtering requires a metadata service with pagination "
                        "and filtering support (at least version %s). Please "
                        "upgrade your service."
                        % cls._MIN_SERVICE_VERSION_WITH_CURSOR_PAGINATION
                    )
                legacy = cls._legacy_get_collection(
                    obj_type,
                    obj_order,
                    sub_type,
                    filters,
                    attempt,
                    *args,
                    raise_on_missing=raise_on_missing,
                )
                for record in legacy or []:
                    yield record
                return

            first_page = False

            for record in MetadataProvider._apply_filter(records, filters):
                yield record

            next_cursor = cls._header_value(headers, cls._NEXT_CURSOR_HEADER)
            if not next_cursor:
                # No further cursor: normal completion.
                return
            if next_cursor in seen_cursors:
                # A repeated cursor is a broken pagination exchange. Returning
                # would hand the caller a truncated listing as if it were complete.
                raise ServiceException(
                    "Metadata service returned a repeated pagination cursor; "
                    "refusing to return a truncated listing."
                )
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    @classmethod
    def _get_object_internal(
        cls, obj_type, obj_order, sub_type, sub_order, filters, attempt, *args
    ):
        if attempt is not None:
            if not cls._service_supports("2.0.6"):
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

        # Newer services can stream collections, but get_object's contract is a
        # concrete object-or-list, so we materialize the pages here to keep every
        # existing caller unchanged. That trades some client-side memory for
        # backward compatibility -- the goal of pagination here is to relieve
        # server-side pressure, not client-side. Callers that need to stream
        # large collections without materializing should go through
        # iter_objects() / _iter_paginated_records (e.g. Flow.runs()), which
        # yield page by page. Older services stay on the bulk GET.
        #
        # Possible follow-up (left for a future contributor): let the generic
        # get_object / MetaflowObject.__iter__ path stream for new services too,
        # so plain iteration is also memory-bounded. That requires changing
        # get_object's object-or-list return contract, so it is intentionally
        # out of scope here.
        if (
            cls._can_paginate_collection(sub_type, attempt)
            and cls._service_supports_cursor_pagination()
        ):
            try:
                return list(
                    cls._iter_paginated_records(
                        obj_type,
                        obj_order,
                        sub_type,
                        filters,
                        attempt,
                        *args,
                        raise_on_missing=True,
                    )
                )
            except ServiceException as ex:
                if ex.http_code == 404:
                    return None
                raise

        return cls._legacy_get_collection(
            obj_type, obj_order, sub_type, filters, attempt, *args
        )

    @classmethod
    def iter_objects(cls, obj_type, sub_type, filters, attempt, *args, **kwargs):
        """Stream collection listings using cursor pagination when the service supports it.

        Like the base implementation, this is a generator: argument validation
        and capability errors surface when iteration starts, not at call time.
        """
        query_filters = kwargs.pop("query_filters", None) or {}
        page_size = kwargs.pop("page_size", None)
        if kwargs:
            raise TypeError("Unexpected iterator options: %s" % ", ".join(kwargs))

        # Validate before anything that touches the network: the capability check
        # below pings the service, so a nonsensical obj_type/sub_type pair must
        # fail locally rather than after a round trip. The paginated path has to
        # reject exactly what the materialized get_object path rejects.
        obj_order, _ = cls._validate_object_query(obj_type, sub_type)

        if not cls._service_supports_cursor_pagination():
            if query_filters:
                raise ServiceException(
                    "Filtering requires a metadata service with pagination "
                    "and filtering support (at least version %s). Please "
                    "upgrade your service."
                    % cls._MIN_SERVICE_VERSION_WITH_CURSOR_PAGINATION
                )
            for obj in super(ServiceMetadataProvider, cls).iter_objects(
                obj_type, sub_type, filters, attempt, *args, page_size=page_size
            ):
                yield obj
            return

        if not cls._can_paginate_collection(sub_type, attempt):
            for obj in super(ServiceMetadataProvider, cls).iter_objects(
                obj_type,
                sub_type,
                filters,
                attempt,
                *args,
                query_filters=query_filters,
                page_size=page_size,
            ):
                yield obj
            return

        for obj in cls._iter_paginated_records(
            obj_type,
            obj_order,
            sub_type,
            filters,
            attempt,
            *args,
            query_filters=query_filters,
            page_size=page_size,
        ):
            yield obj

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

    @classmethod
    def filter_tasks_by_metadata(
        cls,
        flow_name: str,
        run_id: str,
        step_name: str,
        field_name: str,
        pattern: str,
    ) -> List[str]:
        """
        Filter tasks by metadata field and pattern, returning task pathspecs that match criteria.

        Parameters
        ----------
        flow_name : str
            Flow name, that the run belongs to.
        run_id: str
            Run id, together with flow_id, that identifies the specific Run whose tasks to query
        step_name: str
            Step name to query tasks from
        field_name: str
            Metadata field name to query
        pattern: str
            Pattern to match in metadata field value

        Returns
        -------
        List[str]
            List of task pathspecs that satisfy the query
        """
        query_params = {}

        if pattern == ".*":
            # we do not need to filter tasks at all if pattern allows 'any'
            query_params = {}
        else:
            if field_name:
                query_params["metadata_field_name"] = field_name
            if pattern:
                # The service performs an unanchored regex search, so anchor the
                # pattern to match the local provider's fullmatch behavior.
                query_params["pattern"] = "^(?:%s)$" % pattern

        url = ServiceMetadataProvider._obj_path(flow_name, run_id, step_name)
        url = f"{url}/filtered_tasks?{urlencode(query_params)}"

        try:
            resp, _ = cls._request(None, url, "GET")
        except Exception as e:
            if e.http_code == 404:
                # filter_tasks_by_metadata endpoint does not exist in the version of metadata service
                # deployed currently. Raise a more informative error message.
                raise MetaflowInternalError(
                    "The version of metadata service deployed currently does not support filtering tasks by metadata. "
                    "Upgrade Metadata service to version 2.5.0 or greater to use this feature."
                ) from e
            # Other unknown exception
            raise e
        return resp

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
        return_headers=False,
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
                            resp = cls._session.get(url, headers=SERVICE_HEADERS.copy())
                    else:
                        resp = cls._session.get(url, headers=SERVICE_HEADERS.copy())
                elif method == "POST":
                    if monitor:
                        with monitor.measure("metaflow.service_metadata.post"):
                            resp = cls._session.post(
                                url, headers=SERVICE_HEADERS.copy(), json=data
                            )
                    else:
                        resp = cls._session.post(
                            url, headers=SERVICE_HEADERS.copy(), json=data
                        )
                elif method == "PATCH":
                    if monitor:
                        with monitor.measure("metaflow.service_metadata.patch"):
                            resp = cls._session.patch(
                                url, headers=SERVICE_HEADERS.copy(), json=data
                            )
                    else:
                        resp = cls._session.patch(
                            url, headers=SERVICE_HEADERS.copy(), json=data
                        )
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
                    body = resp.json()
                    if return_headers:
                        return body, resp.headers
                    return body, True
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
                        resp = cls._session.get(url, headers=SERVICE_HEADERS.copy())
                else:
                    resp = cls._session.get(url, headers=SERVICE_HEADERS.copy())
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
                elif resp.status_code not in (503, 500):
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
