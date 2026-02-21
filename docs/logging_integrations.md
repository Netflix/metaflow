# Monitoring and Logging Integrations

Metaflow emits logs during flow execution through the CLI and the
runtime environment of individual steps. While Metaflow focuses on
workflow orchestration and reproducibility, teams often integrate
external observability systems to centralize logs and metrics across
their infrastructure.

This document outlines common patterns for forwarding Metaflow logs
and metrics to platforms such as Datadog and Splunk. These examples
are intended as integration guidelines rather than official or
fully supported plugins.

## How Metaflow Emits Logs

Metaflow primarily produces logs through the following mechanisms:

- CLI stdout and stderr output during `python flow.py run`
- Step execution logs produced by task runtime
- Metadata-related logging depending on the configured backend

External monitoring tools typically ingest these logs by capturing
standard output streams or collecting files generated during execution.
Metaflow itself does not enforce a specific observability stack, which
allows users to integrate with existing logging infrastructure.

## Example: Datadog Integration

A common approach for integrating Metaflow with Datadog is to use the
Datadog Agent to collect logs from the execution environment.

Typical workflow:

1. Install and configure the Datadog Agent on the machine running
   Metaflow.
2. Enable log collection for stdout streams or execution log files.
3. Attach tags such as flow name, step name, or run ID for filtering.

For example, logs produced by:
`python flow.py run`
can be captured by the Datadog Agent and forwarded into Datadog log
pipelines using standard agent configuration. Metrics collection can be
added by instrumenting the execution environment alongside log ingestion.

### CloudWatch Forwarder Pattern

When running flows on AWS Batch, Metaflow sends logs to CloudWatch
using the default `awslogs` driver. A common integration pattern is to
forward CloudWatch logs to Datadog using the Datadog Forwarder Lambda.

Metaflow Batch logs are typically written to the `/aws/batch/job`
log group. The associated log streams can be identified by the job
definition name prefix `metaflow_`, which appears within the stream
name when using the default AWS Batch logging configuration. These
logs can be forwarded to Datadog using the official Datadog Forwarder:

https://docs.datadoghq.com/serverless/libraries_integrations/forwarder/

Using the Metaflow Client API, it is also possible to retrieve the
CloudWatch log group and stream associated with a task:

```python
from metaflow import Run

run = Run("MyFlow/42")
run["start"].task.metadata_dict["aws-batch-awslogs-stream"]
run["start"].task.metadata_dict["aws-batch-awslogs-group"]
```

This approach works with the default Metaflow Batch configuration and
does not require modifying the job definition. It provides a simple
bridge between CloudWatch and Datadog without introducing additional
runtime dependencies.

## Example: Splunk Integration

Splunk integrations generally rely on forwarding logs generated during
Metaflow execution using existing Splunk tooling.

Common patterns include:

- Using the Splunk Universal Forwarder to collect CLI output logs
- Monitoring execution log directories for new files
- Enriching logs with metadata such as run ID or step name

These approaches allow Metaflow runs to be analyzed alongside other
operational logs within Splunk dashboards.

## Best Practices

- Prefer structured logging to improve searchability and filtering.
- Include identifiers such as flow name, step name, and run ID.
- Separate development and production logging configurations.
- Avoid forwarding sensitive information to external systems.

## Future Improvements

This document provides a high-level overview of integration patterns.
Future revisions may include concrete configuration examples and
reference setups for commonly used observability platforms.
