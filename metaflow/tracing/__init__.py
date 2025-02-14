import sys
from metaflow.metaflow_config import (
    OTEL_ENDPOINT,
    ZIPKIN_ENDPOINT,
    CONSOLE_TRACE_ENABLED,
    DISABLE_TRACING,
    DEBUG_TRACING,
)
from functools import wraps
import contextlib
from typing import Dict


def init_tracing():
    pass


@contextlib.contextmanager
def post_fork():
    yield


def cli(name: str):
    def cli_wrap(func):
        @wraps(func)
        def wrapper_func(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper_func

    return cli_wrap


def inject_tracing_vars(env_dict: Dict[str, str]) -> Dict[str, str]:
    return env_dict


def get_trace_id() -> str:
    return ""


@contextlib.contextmanager
def traced(name, attrs=None):
    if attrs is None:
        attrs = {}
    yield


def tracing(func):
    @wraps(func)
    def wrapper_func(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper_func


if not DISABLE_TRACING and (CONSOLE_TRACE_ENABLED or OTEL_ENDPOINT or ZIPKIN_ENDPOINT):
    try:
        from .tracing_modules import (
            init_tracing,
            post_fork,
            cli,
            inject_tracing_vars,
            get_trace_id,
            traced,
            tracing,
        )
    except ImportError as e:
        # We keep the errors silent by default so that having tracing environment variables present
        # does not affect users with no need for tracing.
        if DEBUG_TRACING:
            print(e.msg, file=sys.stderr)
