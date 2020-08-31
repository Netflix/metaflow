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
        return message, obj_keys, stream_key, disposable_keys

    @classmethod
    def response(cls, keys_objs):
        return key_objs

    @classmethod
    def stream_response(cls, it):
        return iterator

    @classmethod
    def execute(cls, message, keys, stream_key):
        pass

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
    def execute(cls, message, keys, stream_key):
        return {key: 'works: %s' % key for key in keys}
