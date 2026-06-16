import sys
from metaflow.metaflow_config import (
    OTEL_ENDPOINT,
    ZIPKIN_ENDPOINT,
    CONSOLE_TRACE_ENABLED,
    SERVICE_AUTH_KEY,
    SERVICE_HEADERS,
)


def get_span_exporter():
    exporter_map = {
        OTEL_ENDPOINT: _create_otel_exporter,
        ZIPKIN_ENDPOINT: _create_zipkin_exporter,
        CONSOLE_TRACE_ENABLED: _create_console_exporter,
    }

    for config, create_exporter in exporter_map.items():
        if config:
            return create_exporter()

    print("WARNING: endpoints not set up for OpenTelemetry", file=sys.stderr)
    return None


def _create_otel_exporter():
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    if not any([SERVICE_AUTH_KEY, SERVICE_HEADERS]):
        print("WARNING: no auth settings for OpenTelemetry", file=sys.stderr)
        return None

    config = {
        "endpoint": OTEL_ENDPOINT,
        "timeout": 1,
    }

    if SERVICE_AUTH_KEY:
        config["headers"] = {"x-api-key": SERVICE_AUTH_KEY}
    elif SERVICE_HEADERS:
        config["headers"] = SERVICE_HEADERS

    return OTLPSpanExporter(**config)


def _create_zipkin_exporter():
    from opentelemetry.exporter.zipkin.proto.http import ZipkinExporter

    return ZipkinExporter(endpoint=ZIPKIN_ENDPOINT)


def _create_console_exporter():
    from opentelemetry.sdk.trace.export import ConsoleSpanExporter

    return ConsoleSpanExporter()
