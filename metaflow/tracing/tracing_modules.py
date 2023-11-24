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

tracer_provider = None


def init_tracing():
    global tracer_provider
    if tracer_provider is not None:
        print("Tracing already initialized", file=sys.stderr)
        return

    from .propagator import EnvPropagator

    set_global_textmap(EnvPropagator(None))
    span_exporter = get_span_exporter()

    if "METAFLOW_KUBERNETES_POD_NAMESPACE" in os.environ:
        service_name = "metaflow-kubernetes"
    elif "AWS_BATCH_JOB_ID" in os.environ:
        service_name = "metaflow-awsbatch"
    else:
        service_name = "metaflow-local"

    tracer_provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name})
    )
    trace_api.set_tracer_provider(tracer_provider)

    span_processor = BatchSpanProcessor(span_exporter)
    tracer_provider.add_span_processor(span_processor)

    from opentelemetry.instrumentation.requests import RequestsInstrumentor

    RequestsInstrumentor().instrument()


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
            yield
    finally:
        context.detach(token)


def _extract_token_after(tokens: List[str], before_token: str) -> Optional[str]:
    for i, tok in enumerate(tokens):
        if i > 0 and tokens[i - 1] == before_token:
            return tok


def cli_entrypoint(name: str):
    def cli_entrypoint_wrap(func):
        @wraps(func)
        def wrapper_func(*args, **kwargs):
            global tracer_provider

            init_tracing()

            assert tracer_provider is not None  # make type checker happy

            token = context.attach(extract(os.environ))
            try:
                tracer = trace_api.get_tracer_provider().get_tracer(__name__)

                card_subcommand = _extract_token_after(sys.argv, "card")

                step_name = _extract_token_after(sys.argv, "step")
                task_id = _extract_token_after(sys.argv, "--task-id")
                run_id = _extract_token_after(sys.argv, "--run-id")
                if step_name and task_id and run_id:
                    better_name = "/".join([run_id, step_name, task_id])
                elif card_subcommand:
                    better_name = "card/" + card_subcommand
                elif "run" in sys.argv:
                    better_name = "run"
                else:
                    better_name = None

                with tracer.start_as_current_span(
                    better_name or name, kind=trace_api.SpanKind.SERVER
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

    return cli_entrypoint_wrap


def inject_tracing_vars(env_dict: Dict[str, str]) -> Dict[str, str]:
    inject(env_dict)
    return env_dict


def get_trace_id() -> str:
    return format_trace_id(trace_api.get_current_span().get_span_context().trace_id)


@contextlib.contextmanager
def traced(name, attrs={}):
    tracer = trace_api.get_tracer_provider().get_tracer(__name__)
    with tracer.start_as_current_span(name) as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
        yield


def tracing(func):
    @wraps(func)
    def wrapper_func(*args, **kwargs):
        tracer = trace_api.get_tracer_provider().get_tracer(func.__module__.__name__)

        with tracer.start_as_current_span(func.__name__):
            return func(*args, **kwargs)

    return wrapper_func
