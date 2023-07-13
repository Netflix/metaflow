import sys
from metaflow.metaflow_config import (
    OTEL_ENDPOINT,
    ZIPKIN_ENDPOINT,
    CONSOLE_TRACE_ENABLED,
)


def get_span_exporter():
    if OTEL_ENDPOINT:
        return set_otel_exporter()

    elif ZIPKIN_ENDPOINT:
        return set_zipkin_exporter()

    elif CONSOLE_TRACE_ENABLED:
        return set_console_exporter()
    else:
        print("WARNING: endpoints not set up for Opentelemetry", file=sys.stderr)
        return


def set_otel_exporter():
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    from metaflow.metaflow_config import (
        SERVICE_AUTH_KEY,
        SERVICE_HEADERS,
    )

    if SERVICE_AUTH_KEY:
        span_exporter = OTLPSpanExporter(
            endpoint=OTEL_ENDPOINT,
            headers={"x-api-key": SERVICE_AUTH_KEY},
            timeout=1,
        )
    elif SERVICE_HEADERS:
        span_exporter = OTLPSpanExporter(
            endpoint=OTEL_ENDPOINT,
            headers=SERVICE_HEADERS,
            timeout=1,
        )
    else:
        print("WARNING: no auth settings for Opentelemetry", file=sys.stderr)
        return
    return span_exporter


def set_zipkin_exporter():
    from opentelemetry.exporter.zipkin.proto.http import ZipkinExporter

    span_exporter = ZipkinExporter(
        endpoint=ZIPKIN_ENDPOINT,
    )
    return span_exporter


def set_console_exporter():
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter

    span_exporter = ConsoleSpanExporter()
    return span_exporter
