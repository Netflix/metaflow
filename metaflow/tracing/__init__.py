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
from typing import Dict, Optional


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
    yield None


def tracing(func):
    @wraps(func)
    def wrapper_func(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper_func


def add_span_event(name: str, attrs: Optional[Dict] = None):
    pass


def set_span_attributes(attrs: Dict):
    pass


def get_current_span():
    return None


def flush_tracing():
    pass


def _enable_tracing_modules(exporter=None, span_processor=None):
    try:
        from . import tracing_modules

        if exporter is not None or span_processor is not None:
            tracing_modules.init_tracing(
                exporter=exporter, span_processor=span_processor
            )
        for name in (
            "init_tracing",
            "post_fork",
            "cli",
            "inject_tracing_vars",
            "get_trace_id",
            "traced",
            "tracing",
            "add_span_event",
            "set_span_attributes",
            "get_current_span",
            "flush_tracing",
        ):
            globals()[name] = getattr(tracing_modules, name)
    except ImportError as e:
        if DEBUG_TRACING:
            print(e.msg, file=sys.stderr)


def init_tracing(exporter=None, span_processor=None):
    _enable_tracing_modules(exporter=exporter, span_processor=span_processor)


if not DISABLE_TRACING and (CONSOLE_TRACE_ENABLED or OTEL_ENDPOINT or ZIPKIN_ENDPOINT):
    _enable_tracing_modules()
