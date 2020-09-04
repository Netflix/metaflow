import os
import requests
import time

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import METADATA_SERVICE_NUM_RETRIES, METADATA_SERVICE_HEADERS, \
    METADATA_SERVICE_URL
from .metadata import MetadataProvider


class ServiceException(MetaflowException):
    headline = 'Metaflow service error'

    def __init__(self, msg, http_code=None, body=None):
        self.http_code = None if http_code is None else int(http_code)
        self.response = body
        super(ServiceException, self).__init__(msg)


class ServiceMetadataProvider(MetadataProvider):
    TYPE = 'service'

    def __init__(self, environment, flow, event_logger, monitor):
        super(ServiceMetadataProvider, self).__init__(environment, flow, event_logger, monitor)

    @classmethod
    def compute_info(cls, val):
        v = val.rstrip('/')
        try:
            resp = requests.get(os.path.join(v, 'ping'), headers=METADATA_SERVICE_HEADERS)
            resp.raise_for_status()
        except:  # noqa E722
            raise ValueError('Metaflow service [%s] unreachable.' % v)
        return v

    @classmethod
    def default_info(cls):
        return METADATA_SERVICE_URL

    def version(self):
        return self._version(self._monitor)

    def new_run_id(self, tags=[], sys_tags=[]):
        return self._new_run(tags=tags, sys_tags=sys_tags)

    def register_run_id(self, run_id, tags=[], sys_tags=[]):
        try:
            # don't try to register an integer ID which was obtained
            # from the metadata service in the first place
            int(run_id)
            return
        except ValueError:
            return self._new_run(run_id, tags=tags, sys_tags=sys_tags)

    def new_task_id(self, run_id, step_name, tags=[], sys_tags=[]):
        return self._new_task(run_id, step_name, tags=tags, sys_tags=sys_tags)

    def register_task_id(self,
                         run_id,
                         step_name,
                         task_id,
                         tags=[],
                         sys_tags=[]):
        try:
            # don't try to register an integer ID which was obtained
            # from the metadata service in the first place
            int(task_id)
        except ValueError:
            self._new_task(run_id,
                           step_name,
                           task_id,
                           tags=tags,
                           sys_tags=sys_tags)
        finally:
            self._register_code_package_metadata(run_id, step_name, task_id)

        
    def get_runtime_environment(self, runtime_name):
        return {}

    def register_data_artifacts(self,
                                run_id,
                                step_name,
                                task_id,
                                attempt_id,
                                artifacts):
        url = ServiceMetadataProvider._obj_path(self._flow_name, run_id, step_name, task_id)
        url += '/artifact'
        data = self._artifacts_to_json(run_id, step_name, task_id, attempt_id, artifacts)
        self._request(self._monitor, url, data)

    def register_metadata(self, run_id, step_name, task_id, metadata):
        url = ServiceMetadataProvider._obj_path(self._flow_name, run_id, step_name, task_id)
        url += '/metadata'
        data = self._metadata_to_json(run_id, step_name, task_id, metadata)
        self._request(self._monitor, url, data)

    @classmethod
    def _get_object_internal(cls, obj_type, obj_order, sub_type, sub_order, filters=None, *args):
        # Special handling of self, artifact, and metadata
        if sub_type == 'self':
            url = ServiceMetadataProvider._obj_path(*args[:obj_order])
            try:
                return MetadataProvider._apply_filter([cls._request(None, url)], filters)[0]
            except ServiceException as ex:
                if ex.http_code == 404:
                    return None
                raise

        # For the other types, we locate all the objects we need to find and return them
        if obj_type != 'root':
            url = ServiceMetadataProvider._obj_path(*args[:obj_order])
        else:
            url = ''
        if sub_type != 'metadata':
            url += '/%ss' % sub_type
        else:
            url += '/metadata'
        try:
            return MetadataProvider._apply_filter(cls._request(None, url), filters)
        except ServiceException as ex:
            if ex.http_code == 404:
                return None
            raise

    def _new_run(self, run_id=None, tags=[], sys_tags=[]):
        # first ensure that the flow exists
        self._get_or_create('flow')
        run = self._get_or_create('run', run_id, tags=tags, sys_tags=sys_tags)
        return str(run['run_number'])

    def _new_task(self,
                  run_id,
                  step_name,
                  task_id=None,
                  tags=[],
                  sys_tags=[]):
        # first ensure that the step exists
        self._get_or_create('step', run_id, step_name)
        task = self._get_or_create('task', run_id, step_name, task_id, tags=tags, sys_tags=sys_tags)
        self._register_code_package_metadata(run_id, step_name, task['task_id'])
        return task['task_id']

    @staticmethod
    def _obj_path(
            flow_name, run_id=None, step_name=None, task_id=None, artifact_name=None):
        object_path = '/flows/%s' % flow_name
        if run_id:
            object_path += '/runs/%s' % run_id
        if step_name:
            object_path += '/steps/%s' % step_name
        if task_id:
            object_path += '/tasks/%s' % task_id
        if artifact_name:
            object_path += '/artifacts/%s' % artifact_name
        return object_path

    @staticmethod
    def _create_path(obj_type, flow_name, run_id=None, step_name=None):
        create_path = '/flows/%s' % flow_name
        if obj_type == 'flow':
            return create_path
        if obj_type == 'run':
            return create_path + '/run'
        create_path += '/runs/%s/steps/%s' % (run_id, step_name)
        if obj_type == 'step':
            return create_path + '/step'
        return create_path + '/task'

    def _get_or_create(
            self, obj_type, run_id=None, step_name=None, task_id=None, tags=[], sys_tags=[]):

        def create_object():
            data = self._object_to_json(
                obj_type,
                run_id,
                step_name,
                task_id,
                tags + self.sticky_tags,
                sys_tags + self.sticky_sys_tags)
            return self._request(self._monitor, create_path, data, obj_path)

        always_create = False
        obj_path = self._obj_path(self._flow_name, run_id, step_name, task_id)
        create_path = self._create_path(obj_type, self._flow_name, run_id, step_name)
        if obj_type == 'run' and run_id is None:
            always_create = True
        elif obj_type == 'task' and task_id is None:
            always_create = True

        if always_create:
            return create_object()

        try:
            return self._request(self._monitor, obj_path)
        except ServiceException as ex:
            if ex.http_code == 404:
                return create_object()
            else:
                raise

    @classmethod
    def _request(cls, monitor, path, data=None, retry_409_path=None):
        if cls.INFO is None:
            raise MetaflowException('Missing Metaflow Service URL. '
                'Specify with METAFLOW_SERVICE_URL environment variable')
        url = os.path.join(cls.INFO, path.lstrip('/'))
        for i in range(METADATA_SERVICE_NUM_RETRIES):
            try:
                if data is None:
                    if monitor:
                        with monitor.measure('metaflow.service_metadata.get'):
                            resp = requests.get(url, headers=METADATA_SERVICE_HEADERS)
                    else:
                        resp = requests.get(url, headers=METADATA_SERVICE_HEADERS)
                else:
                    if monitor:
                        with monitor.measure('metaflow.service_metadata.post'):
                            resp = requests.post(url, headers=METADATA_SERVICE_HEADERS, json=data)
                    else:
                        resp = requests.post(url, headers=METADATA_SERVICE_HEADERS, json=data)
            except:  # noqa E722
                if monitor:
                    with monitor.count('metaflow.service_metadata.failed_request'):
                        if i == METADATA_SERVICE_NUM_RETRIES - 1:
                            raise
                else:
                    if i == METADATA_SERVICE_NUM_RETRIES - 1:
                        raise
                resp = None
            else:
                if resp.status_code < 300:
                    return resp.json()
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
                        return cls._request(monitor, retry_409_path)
                    else:
                        return
                elif resp.status_code != 503:
                    raise ServiceException('Metadata request (%s) failed (code %s): %s'
                                           % (path, resp.status_code, resp.text),
                                           resp.status_code,
                                           resp.text)
            time.sleep(2**i)

        if resp:
            raise ServiceException('Metadata request (%s) failed (code %s): %s'
                                   % (path, resp.status_code, resp.text),
                                   resp.status_code,
                                   resp.text)
        else:
            raise ServiceException('Metadata request (%s) failed' % path)

    @classmethod
    def _version(cls, monitor):
        if cls.INFO is None:
            raise MetaflowException('Missing Metaflow Service URL. '
                'Specify with METAFLOW_SERVICE_URL environment variable')
        url = os.path.join(cls.INFO, 'ping')
        for i in range(METADATA_SERVICE_NUM_RETRIES):
            try:
                if monitor:
                    with monitor.measure('metaflow.service_metadata.get'):
                        resp = requests.get(url,
                            headers=METADATA_SERVICE_HEADERS)
                else:
                    resp = requests.get(url, headers=METADATA_SERVICE_HEADERS)
            except:
                if monitor:
                    with monitor.count(
                        'metaflow.service_metadata.failed_request'):
                        if i == METADATA_SERVICE_NUM_RETRIES - 1:
                            raise
                else:
                    if i == METADATA_SERVICE_NUM_RETRIES - 1:
                        raise
                resp = None
            else:
                if resp.status_code < 300:
                    return resp.headers.get('METADATA_SERVICE_VERSION', None)
                elif resp.status_code != 503:
                    raise ServiceException('Metadata request (%s) failed'
                                           ' (code %s): %s' %
                                           (path, resp.status_code, resp.text),
                                           resp.status_code,
                                           resp.text)
            time.sleep(2**i)
        if resp:
            raise ServiceException('Metadata request (%s) failed (code %s): %s'
                                   % (path, resp.status_code, resp.text),
                                   resp.status_code,
                                   resp.text)
        else:
            raise ServiceException('Metadata request (%s) failed' % path)