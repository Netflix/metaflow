from functools import wraps
import contextlib
from typing import Dict


def init_tracing():
    pass


@contextlib.contextmanager
def post_fork():
    yield


def cli_entrypoint(name: str):
    def cli_entrypoint_wrap(func):
        @wraps(func)
        def wrapper_func(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper_func

    return cli_entrypoint_wrap


def inject_tracing_vars(env_dict: Dict[str, str]) -> Dict[str, str]:
    return env_dict


def get_trace_id() -> str:
    return ""


@contextlib.contextmanager
def traced(name, attrs={}):
    yield


def tracing(func):
    @wraps(func)
    def wrapper_func(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper_func
