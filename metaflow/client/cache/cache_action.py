import uuid
import importlib

LO_PRIO = 'lo_prio'
HI_PRIO = 'hi_prio'

class CacheServerInitFailed(Exception):
    pass

def import_action_class_spec(action_spec):
    parts = action_spec.split('.')
    package = '.'.join(action_spec.split('.')[:-1])
    action_name = action_spec.split('.')[-1]
    return import_action_class('.'.join(parts[:-1]), parts[-1])

def import_action_class(mod, cls):
    return getattr(importlib.import_module(mod), cls)

class CacheAction(object):

    PRIORITY = LO_PRIO

    @classmethod
    def format_request(cls, *args, **kwargs):
        """
        Encode the given arguments as a request. This method
        is proxied by `cache_client` as a client-facing API
        of the action.

        Function returns a four-tuple:
        1. `message`: an arbitrary JSON-encodable payload that
           is passed to `execute`.
        2. `obj_keys`: a list of keys that the action promises
           to produce in `execute`.
        3. `stream_key`: an optional key name for a streaming
           result of the action. May be `None` if the action
           doesn't have any streaming results.
        4. `disposable_keys`: a subset of `obj_keys` that will
           be purged from the cache before other objects.
        """
        #return message, obj_keys, stream_key, disposable_keys
        raise NotImplementedError

    @classmethod
    def response(cls, keys_objs):
        """
        Decodes and refines `execute` output before it is returned
        to the client. The argument `keys_objs` is the return value
        of `execute`. This method is called by `cache_client` to
        convert serialized, cached results to a client-facing object.

        The function may return anything.
        """
        raise NotImplementedError

    @classmethod
    def stream_response(cls, it):
        """
        Iterator that iterates over streamed events in `it`. This
        generator is the reader counterpart to the `stream_output`
        writer in `execute`. This method is called by `cache_client`
        to convert serialized events to client-facing objects.

        If the event is `None`, it should be yield as-is. For other
        events, the function may perform any stateful manipulation and
        yield zero or more refined objects.
        """
        raise NotImplementedError

    @classmethod
    def execute(cls,
                message=None,
                keys=[],
                existing_keys={},
                stream_output=None):
        """
        Execute an action. This method is called by `cache_worker` to
        execute the action as a subprocess.

        - `message` is an arbitrary payload produced by format_request.
        - `keys` is a list of objects that the action needs to produce.
        - `existing_keys` refers to existing values of caches keys, if
          available.
        - `stream_output` is a function that can be called to produce
          an output event to the stream object.

        Returns a dictionary that includes a string/byte result
        per key that will be stored in the cache.
        """
        raise NotImplementedError

class Check(CacheAction):

    PRIORITY = HI_PRIO

    @classmethod
    def format_request(cls, *args, **kwargs):
        key = 'check-%s' % uuid.uuid4()
        return None, [key], None, [key]

    @classmethod
    def response(cls, keys_objs):
        for key, blob in keys_objs.items():
            if blob != b'works: %s' % key.encode('utf-8'):
                raise CacheServerInitFailed()
        return True

    @classmethod
    def stream_response(cls, it):
        pass

    @classmethod
    def execute(cls, keys=[], **kwargs):
        return {key: 'works: %s' % key for key in keys}
