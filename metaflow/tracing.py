import sys
from metaflow.metaflow_config import (
    OTEL_ENDPOINT,
)

import metaflow.tracing_noop

init_tracing = metaflow.tracing_noop.init_tracing
cli_entrypoint = metaflow.tracing_noop.cli_entrypoint
inject_tracing_vars = metaflow.tracing_noop.inject_tracing_vars
get_trace_id = metaflow.tracing_noop.get_trace_id
traced = metaflow.tracing_noop.traced
tracing = metaflow.tracing_noop.tracing
post_fork = metaflow.tracing_noop.post_fork

if OTEL_ENDPOINT:
    try:
        import metaflow.tracing_otel

        init_tracing = metaflow.tracing_otel.init_tracing
        cli_entrypoint = metaflow.tracing_otel.cli_entrypoint
        inject_tracing_vars = metaflow.tracing_otel.inject_tracing_vars
        get_trace_id = metaflow.tracing_otel.get_trace_id
        traced = metaflow.tracing_otel.traced
        tracing = metaflow.tracing_otel.tracing
        post_fork = metaflow.tracing_otel.post_fork
    except ImportError:
        pass
