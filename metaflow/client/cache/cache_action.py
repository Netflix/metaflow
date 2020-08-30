
import importlib

LO_PRIO = 'lo_prio'
HI_PRIO = 'hi_prio'

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
        return message, obj_keys, stream_key, gc_timestamps

    @classmethod
    def response(cls, keys_objs):
        return key_objs

    @classmethod
    def stream_response(cls, it):
        return iterator

    @classmethod
    def execute(cls, message, keys, stream_key):
        pass

