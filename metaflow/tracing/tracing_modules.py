import os
import sys

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.trace.span import format_trace_id
from opentelemetry.propagate import extract, inject
from functools import wraps
import contextlib
from typing import Dict, List, Optional
from opentelemetry import trace as trace_api, context
from .span_exporter import get_span_exporter

tracer_provider: Optional[TracerProvider] = None


def init_tracing():
    global tracer_provider
    if tracer_provider is not None:
        return

    from .propagator import EnvPropagator

    set_global_textmap(EnvPropagator(None))

    span_exporter = get_span_exporter()
    if span_exporter is None:
        return

    tracer_provider = TracerProvider(
        resource=Resource.create(
            {
                SERVICE_NAME: "metaflow",
            }
        )
    )
    trace_api.set_tracer_provider(tracer_provider)

    span_processor = BatchSpanProcessor(span_exporter)
    tracer_provider.add_span_processor(span_processor)

    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor

        RequestsInstrumentor().instrument(
            tracer_provider=tracer_provider,
        )
    except ImportError:
        pass


@contextlib.contextmanager
def post_fork():
    global tracer_provider

    tracer_provider = None
    init_tracing()

    token = context.attach(extract(os.environ))
    try:
        tracer = trace_api.get_tracer_provider().get_tracer(__name__)
        with tracer.start_as_current_span(
            "fork", kind=trace_api.SpanKind.SERVER
        ) as span:
            span.set_attribute("cmd", " ".join(sys.argv))
            span.set_attribute("pid", str(os.getpid()))
            yield
    finally:
        context.detach(token)


def cli(name: str):
    def cli_wrap(func):
        @wraps(func)
        def wrapper_func(*args, **kwargs):
            global tracer_provider
            init_tracing()

            if tracer_provider is None:
                return func(*args, **kwargs)

            token = context.attach(extract(os.environ))
            try:
                tracer = trace_api.get_tracer_provider().get_tracer(__name__)
                with tracer.start_as_current_span(
                    name, kind=trace_api.SpanKind.SERVER
                ) as span:
                    span.set_attribute("cmd", " ".join(sys.argv))
                    span.set_attribute("pid", str(os.getpid()))
                    return func(*args, **kwargs)
            finally:
                context.detach(token)
                try:
                    tracer_provider.force_flush()
                except Exception as e:  # pylint: disable=broad-except
                    print("TracerProvider failed to flush traces", e, file=sys.stderr)

        return wrapper_func

    return cli_wrap


def inject_tracing_vars(env_dict: Dict[str, str]) -> Dict[str, str]:
    inject(env_dict)
    return env_dict


def get_trace_id() -> str:
    try:
        return format_trace_id(trace_api.get_current_span().get_span_context().trace_id)
    except Exception:
        return ""


@contextlib.contextmanager
def traced(name: str, attrs: Optional[Dict] = None):
    if tracer_provider is None:
        yield
        return
    tracer = trace_api.get_tracer_provider().get_tracer(__name__)
    with tracer.start_as_current_span(name) as span:
        if attrs:
            for k, v in attrs.items():
                span.set_attribute(k, v)
        yield


def tracing(func):
    @wraps(func)
    def wrapper_func(*args, **kwargs):
        if tracer_provider is None:
            return func(*args, **kwargs)

        tracer = trace_api.get_tracer_provider().get_tracer(func.__module__)
        with tracer.start_as_current_span(func.__name__):
            return func(*args, **kwargs)

    return wrapper_func
