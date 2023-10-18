from metaflow.metaflow_config import (
    OTEL_ENDPOINT,
    ZIPKIN_ENDPOINT,
    CONSOLE_TRACE_ENABLED,
    DISABLE_TRACING,
)
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


if not DISABLE_TRACING and (CONSOLE_TRACE_ENABLED or OTEL_ENDPOINT or ZIPKIN_ENDPOINT):
    try:
        # Overrides No-Op implementations if a specific provider is configured.
        from .tracing_modules import (
            init_tracing,
            post_fork,
            cli_entrypoint,
            inject_tracing_vars,
            get_trace_id,
            traced,
            tracing,
        )

    except ImportError as e:
        print(e.msg)
