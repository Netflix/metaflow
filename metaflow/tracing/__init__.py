import sys
from metaflow.metaflow_config import (
    OTEL_ENDPOINT,
    ZIPKIN_ENDPOINT,
    TRACING_URL_TEMPLATE,
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


def get_tracing_url() -> str:
    # Do not return anything if tracing is disabled.
    if DISABLE_TRACING:
        return ""
    # Do not return anything if no trace id is available.
    if not (TRACING_URL_TEMPLATE and get_trace_id()):
        return ""
    return TRACING_URL_TEMPLATE.format(trace_id=get_trace_id())


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
        # We keep the errors silent by default so that having tracing environment variables present
        # does not affect users with no need for tracing.
        if DEBUG_TRACING:
            print(e.msg, file=sys.stderr)
