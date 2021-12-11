import os
from posix import environ
import sys
from functools import wraps
import contextlib
from typing import Dict, List, Optional
from opentelemetry import trace as trace_api, context
from opentelemetry.propagate import extract, inject
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from metaflow.metaflow_config import METADATA_SERVICE_AUTH_KEY, METADATA_SERVICE_HEADERS
from opentelemetry.propagate import set_global_textmap
from metaflow.tracing_propagator import EnvPropagator
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

tracer_provider = None


def init_tracing():
    global tracer_provider
    if tracer_provider is not None:
        print("Tracing already initialized", file=sys.stderr)
        return
    # print("initializing tracing", os.environ.get("traceparent"))

    set_global_textmap(EnvPropagator(None))

    if METADATA_SERVICE_AUTH_KEY:
        span_exporter = OTLPSpanExporter(
            endpoint="https://otel.oleg2.dev.outerbounds.xyz/v1/traces",
            headers={"x-api-key": METADATA_SERVICE_AUTH_KEY},
            timeout=1,
        )
    elif METADATA_SERVICE_HEADERS:
        span_exporter = OTLPSpanExporter(
            endpoint="https://otel.oleg2.dev.outerbounds.xyz/v1/traces",
            headers=METADATA_SERVICE_HEADERS,
            timeout=1,
        )
    else:
        print("WARNING: no auth settings for Opentelemetry", file=sys.stderr)
        return

    if "METAFLOW_KUBERNETES_POD_NAMESPACE" in os.environ:
        service_name = "kubernetes"
    elif "AWS_BATCH_JOB_ID" in os.environ:
        service_name = "awsbatch"
    else:
        service_name = "local"

    tracer_provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name})
    )
    trace_api.set_tracer_provider(tracer_provider)

    span_processor = BatchSpanProcessor(span_exporter)
    tracer_provider.add_span_processor(span_processor)


@contextlib.contextmanager
def post_fork():
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


def extract_token_after(tokens: List[str], before_token: str) -> Optional[str]:
    for i, tok in enumerate(tokens):
        if i > 0 and tokens[i - 1] == before_token:
            return tok


def cli_entrypoint(name: str):
    def cli_entrypoint_wrap(func):
        @wraps(func)
        def wrapper_func(*args, **kwargs):
            init_tracing()
            # print("CLI ENTRYPOINT traceparent", os.environ.get("traceparent"), file=sys.stderr)
            token = context.attach(extract(os.environ))
            try:
                tracer = trace_api.get_tracer_provider().get_tracer(__name__)

                step_name = extract_token_after(sys.argv, "step")
                task_id = extract_token_after(sys.argv, "--task-id")
                run_id = extract_token_after(sys.argv, "--run-id")
                if step_name and task_id and run_id:
                    better_name = "/".join([run_id, step_name, task_id])
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

        return wrapper_func

    return cli_entrypoint_wrap


def inject_tracing_vars(env_dict: Dict[str, str]) -> Dict[str, str]:
    inject(env_dict)
    return env_dict


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
