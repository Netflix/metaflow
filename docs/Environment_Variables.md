# Metaflow Environment Variables Reference

**Complete Guide to Metaflow Configuration Environment Variables**

*Revised & verified against metaflow_config.py source code*

This document provides accurate documentation for all environment variables supported by Metaflow, verified against the metaflow_config.py source file. These variables allow you to customize Metaflow's behavior without modifying code.

## Overview

All Metaflow environment variables follow the naming convention: `METAFLOW_<VARIABLE_NAME>`. For example, to set `DEFAULT_DATASTORE`, use the environment variable `METAFLOW_DEFAULT_DATASTORE`.

## Configuration Resolution Order

Variables are resolved in this priority order (highest to lowest):

1. **Shell environment**: `export METAFLOW_VARIABLE=value`
2. **Project config file**: `.metaflow/config.json` in your project directory
3. **Compiled default** (from source code)

> ⚠️ **Important — config.json key naming:** Keys inside `config.json` omit the `METAFLOW_` prefix. For example, use `"DEFAULT_DATASTORE": "s3"` inside the JSON file, **not** `"METAFLOW_DEFAULT_DATASTORE"`.

> ℹ️ **Extension system:** Metaflow supports extensions that can override any config variable at import time by loading modules via the extension_support system. If a variable does not take effect as expected despite being set correctly, a loaded extension may be overriding it.

> ⚠️ **DISABLE_TRACING exception:** `METAFLOW_DISABLE_TRACING` is the only variable that bypasses the `from_conf()` system entirely — it reads directly from `os.environ`. Setting it in `config.json` will **not** work; it must be a real shell environment variable.

## macOS Fork Safety

On macOS, Metaflow automatically sets `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` to prevent crashes caused by Objective-C initialization in forked processes. This is applied unconditionally at import time.

## Hardcoded Constants

The following values are not configurable via environment variables but are important for understanding Metaflow's file layout and behavior:

| Field | Value | Notes |
|---|---|---|
| `DATASTORE_LOCAL_DIR` | `.metaflow` | Fixed name for the local artifact store directory in your project |
| `DATASTORE_SPIN_LOCAL_DIR` | `.metaflow_spin` | Fixed name for the Spin-specific local store |
| `LOCAL_CONFIG_FILE` | `config.json` | Config file name inside the `.metaflow/` directory |
| `MAX_ATTEMPTS` | `6` (hardcoded) | Maximum total task attempts (including retries and fallback). Cannot exceed 99 due to lexicographic ordering of attempt files. Increasing this value has real performance implications for all task datastore lookups. |

---

## Table of Contents

1. [Default Configuration](#default-configuration)
2. [Datastore Configuration](#datastore-configuration)
3. [Client Cache Configuration](#client-cache-configuration)
4. [S3/Cloud Storage Configuration](#s3cloud-storage-configuration)
5. [Secrets Management](#secrets-management)
6. [Metadata Service Configuration](#metadata-service-configuration)
7. [Container & Execution Configuration](#container--execution-configuration)
8. [Cards Configuration](#cards-configuration)
9. [AWS Batch Configuration](#aws-batch-configuration)
10. [AWS Step Functions Configuration](#aws-step-functions-configuration)
11. [Kubernetes Configuration](#kubernetes-configuration)
12. [Argo Workflows Configuration](#argo-workflows-configuration)
13. [Argo Events Configuration](#argo-events-configuration)
14. [Airflow Configuration](#airflow-configuration)
15. [Conda Configuration](#conda-configuration)
16. [Spin Configuration](#spin-configuration)
17. [Debugging & Profiling](#debugging--profiling)
18. [AWS Sandbox Configuration](#aws-sandbox-configuration)
19. [Tracing Configuration](#tracing-configuration)
20. [Feature Flags & Advanced Options](#feature-flags--advanced-options)
21. [Plugin Configuration](#plugin-configuration)
22. [Hidden / Advanced Variables](#hidden--advanced-variables)

---

## Default Configuration

Variables that control Metaflow's default behavior for storage, execution environment, plugins, and cloud clients.

### `METAFLOW_DEFAULT_DATASTORE`
- **Type**: String
- **Default**: `local`
- **Allowed Values**: `local`, `s3`, `azure`, `gs`
- **Use Case**: Choose the storage backend based on your infrastructure.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_DATASTORE=s3
  ```

### `METAFLOW_DEFAULT_ENVIRONMENT`
- **Type**: String
- **Default**: `local`
- **Allowed Values**: Plugin-dependent. Available values depend on which environment plugins are installed and enabled (e.g. `local`, `conda`, `pypi`). Not a fixed enumeration.
- **Note**: The allowed values are not statically defined in core Metaflow — they depend on which environment plugins are installed and enabled.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_ENVIRONMENT=local
  ```

### `METAFLOW_DEFAULT_METADATA`
- **Type**: String
- **Default**: `local`
- **Allowed Values**: `local`, `service`
- **Use Case**: Choose between local SQLite or centralized metadata service.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_METADATA=service
  ```

### `METAFLOW_DEFAULT_EVENT_LOGGER`
- **Type**: String
- **Default**: `nullSidecarLogger`
- **Allowed Values**: `nullSidecarLogger`, `json`, `logging`, etc. (plugin-dependent)
- **Use Case**: Configure event logging for flow execution.

### `METAFLOW_DEFAULT_MONITOR`
- **Type**: String
- **Default**: `nullSidecarMonitor`
- **Use Case**: Enable flow execution monitoring and metrics collection.

### `METAFLOW_DEFAULT_PACKAGE_SUFFIXES`
- **Type**: String (comma-separated)
- **Default**: `.py,.R,.RDS`
- **Use Case**: Include additional file types in code packages.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_PACKAGE_SUFFIXES=.py,.R,.RDS,.yaml
  ```

### `METAFLOW_DEFAULT_AWS_CLIENT_PROVIDER`
- **Type**: String
- **Default**: `boto3`
- **Allowed Values**: `boto3`
- **Use Case**: Specify AWS SDK implementation.

### `METAFLOW_DEFAULT_AZURE_CLIENT_PROVIDER`
- **Type**: String
- **Default**: `azure-default`
- **Use Case**: Configure Azure SDK implementation.

### `METAFLOW_DEFAULT_GCP_CLIENT_PROVIDER`
- **Type**: String
- **Default**: `gcp-default`
- **Use Case**: Configure GCP SDK implementation.

### `METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE`
- **Type**: String
- **Default**: None (optional)
- **Allowed Values**: `aws-secrets-manager`, `gcp-secrets`, `azure-keyvault`
- **Use Case**: Choose secrets management backend.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE=aws-secrets-manager
  ```

### `METAFLOW_DEFAULT_SECRETS_ROLE`
- **Type**: String
- **Default**: None (optional)
- **Use Case**: Specify IAM role or identity for accessing secrets.

### `METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL`
- **Type**: String
- **Default**: `argo-workflows`
- **Allowed Values**: `argo-workflows`, `step-functions`
- **Use Case**: Select deployment backend.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_FROM_DEPLOYMENT_IMPL=step-functions
  ```

### `METAFLOW_DEFAULT_DECOSPECS`
- **Type**: String (space-separated)
- **Default**: `""` (empty)
- **Use Case**: Decorator specs applied globally to every flow, equivalent to passing `--with` on the CLI.
- **Note**: This variable is also dynamically populated by loaded extension modules if they define `TOGGLE_DECOSPECS`. If not explicitly set, the extension-provided specs are used.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_DECOSPECS="resources:cpu=2 batch"
  ```

### `METAFLOW_USER`
- **Type**: String
- **Default**: None (uses system user)
- **Use Case**: Track flow ownership and operations.
- **Example**:
  ```bash
  export METAFLOW_USER=data-scientist
  ```

### `METAFLOW_UI_URL`
- **Type**: String (URL)
- **Default**: None
- **Use Case**: URL of your organization's Metaflow UI — used for display and links in CLI output.
- **Example**:
  ```bash
  export METAFLOW_UI_URL=https://metaflow.mycompany.com
  ```

### `METAFLOW_CONTACT_INFO`
- **Type**: JSON dictionary
- **Default**: `{"Read the documentation": "http://docs.metaflow.org", ...}`
- **Use Case**: Contact information shown when running the `metaflow` command.
- **Example**:
  ```bash
  export METAFLOW_CONTACT_INFO='{"Slack": "https://slack.mycompany.com", "Email": "ml@mycompany.com"}'
  ```

---

## Datastore Configuration

Configuration for artifact and metadata storage backends.

### `METAFLOW_DATASTORE_SYSROOT_LOCAL`
- **Type**: String (path)
- **Default**: None
- **Use Case**: Root directory for local artifact storage.
- **Example**:
  ```bash
  export METAFLOW_DATASTORE_SYSROOT_LOCAL=/data/metaflow
  ```

### `METAFLOW_DATASTORE_SYSROOT_S3`
- **Type**: String
- **Default**: None
- **Format**: `s3://bucket-name/prefix`
- **Example**:
  ```bash
  export METAFLOW_DATASTORE_SYSROOT_S3=s3://my-metaflow-bucket/artifacts
  ```

### `METAFLOW_DATASTORE_SYSROOT_AZURE`
- **Type**: String
- **Default**: None
- **Format**: `abfs://container@account.blob.core.windows.net/prefix`
- **Example**:
  ```bash
  export METAFLOW_DATASTORE_SYSROOT_AZURE=abfs://metaflow@myaccount.blob.core.windows.net/artifacts
  ```

### `METAFLOW_DATASTORE_SYSROOT_GS`
- **Type**: String
- **Default**: None
- **Format**: `gs://bucket-name/prefix`
- **Example**:
  ```bash
  export METAFLOW_DATASTORE_SYSROOT_GS=gs://my-metaflow-bucket/artifacts
  ```

### `METAFLOW_DATASTORE_SYSROOT_SPIN`
- **Type**: String (path)
- **Default**: None
- **Use Case**: Root directory for Spin-specific datastore.
- **Example**:
  ```bash
  export METAFLOW_DATASTORE_SYSROOT_SPIN=/data/metaflow-spin
  ```

### `METAFLOW_ARTIFACT_LOCALROOT`
- **Type**: String (path)
- **Default**: `os.getcwd()` (current working directory)
- **Use Case**: Local root directory for pulling S3 or Azure artifacts to disk.
- **Example**:
  ```bash
  export METAFLOW_ARTIFACT_LOCALROOT=/tmp/metaflow_artifacts
  ```

---

## Client Cache Configuration

Configuration for caching flow and task datastores on the client side.

### `METAFLOW_CLIENT_CACHE_PATH`
- **Type**: String (path)
- **Default**: `/tmp/metaflow_client`
- **Use Case**: Directory for client-side artifact cache.
- **Example**:
  ```bash
  export METAFLOW_CLIENT_CACHE_PATH=/var/cache/metaflow
  ```

### `METAFLOW_CLIENT_CACHE_MAX_SIZE`
- **Type**: Integer
- **Default**: `10000`
- **Use Case**: Maximum number of entries in the client cache.
- **Note**: ⚠️ This is a **count of items**, not a byte size. The value `10000` means caching up to 10,000 items.
- **Example**:
  ```bash
  export METAFLOW_CLIENT_CACHE_MAX_SIZE=50000
  ```

### `METAFLOW_CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT`
- **Type**: Integer
- **Default**: `50`
- **Use Case**: Maximum number of cached FlowDatastore objects.
- **Example**:
  ```bash
  export METAFLOW_CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT=100
  ```

### `METAFLOW_CLIENT_CACHE_MAX_TASKDATASTORE_COUNT`
- **Type**: Integer
- **Default**: `CLIENT_CACHE_MAX_FLOWDATASTORE_COUNT * 100` (5000 by default)
- **Use Case**: Maximum number of cached TaskDatastore objects.
- **Example**:
  ```bash
  export METAFLOW_CLIENT_CACHE_MAX_TASKDATASTORE_COUNT=10000
  ```

---

## S3/Cloud Storage Configuration

Configuration for cloud storage operations, retry policies, and datatools.

### `METAFLOW_S3_ENDPOINT_URL`
- **Type**: String (URL)
- **Default**: None (uses AWS S3)
- **Use Case**: Use S3-compatible storage service instead of AWS S3 (e.g. MinIO).
- **Example**:
  ```bash
  export METAFLOW_S3_ENDPOINT_URL=http://minio.example.com:9000
  ```

### `METAFLOW_S3_VERIFY_CERTIFICATE`
- **Type**: String or Boolean
- **Default**: None (use default SSL verification)
- **Allowed Values**: `True`, `False`, or a path to a CA bundle file
- **Note**: boto3 accepts `False` to disable verification, `True` to use the default CA bundle, or a string path to a custom CA bundle. This is **not** a simple boolean — a path string is also valid.
- **Example**:
  ```bash
  export METAFLOW_S3_VERIFY_CERTIFICATE=False
  ```

### `METAFLOW_S3_SERVER_SIDE_ENCRYPTION`
- **Type**: String
- **Default**: None
- **Allowed Values**: `AES256`, `aws:kms`, etc.
- **Use Case**: Enable encryption for S3 stored artifacts.
- **Example**:
  ```bash
  export METAFLOW_S3_SERVER_SIDE_ENCRYPTION=AES256
  ```

### `METAFLOW_S3_RETRY_COUNT`
- **Type**: Integer
- **Default**: `7`
- **Note**: This is the number of *retries* — setting to `0` means each operation is tried exactly once.
- **Example**:
  ```bash
  export METAFLOW_S3_RETRY_COUNT=3
  ```

### `METAFLOW_S3_WORKER_COUNT`
- **Type**: Integer
- **Default**: `64`
- **Use Case**: Number of concurrent workers for parallel S3 upload/download.
- **Example**:
  ```bash
  export METAFLOW_S3_WORKER_COUNT=32
  ```

### `METAFLOW_S3_TRANSIENT_RETRY_COUNT`
- **Type**: Integer
- **Default**: `20`
- **Use Case**: Retries for transient failures (e.g. SlowDown errors). Total attempts can be up to `(S3_RETRY_COUNT+1) * (S3_TRANSIENT_RETRY_COUNT+1)`.
- **Example**:
  ```bash
  export METAFLOW_S3_TRANSIENT_RETRY_COUNT=30
  ```

### `METAFLOW_S3_LOG_TRANSIENT_RETRIES`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Log transient retry messages to stdout.
- **Example**:
  ```bash
  export METAFLOW_S3_LOG_TRANSIENT_RETRIES=True
  ```

### `METAFLOW_S3_CLIENT_RETRY_CONFIG`
- **Type**: JSON dictionary
- **Default**: `{"max_attempts": 10, "mode": "adaptive"}`
- **Use Case**: Fine-tune AWS SDK retry behavior.
- **Example**:
  ```bash
  export METAFLOW_S3_CLIENT_RETRY_CONFIG='{"max_attempts": 15, "mode": "adaptive"}'
  ```

### `METAFLOW_DATATOOLS_SUFFIX`
- **Type**: String
- **Default**: `data`
- **Use Case**: Suffix appended to the datastore root to derive the datatools path.
- **Example**:
  ```bash
  export METAFLOW_DATATOOLS_SUFFIX=artifacts
  ```

### `METAFLOW_DATATOOLS_S3ROOT`
- **Type**: String
- **Default**: `<DATASTORE_SYSROOT_S3>/data` (auto-derived)
- **Use Case**: Override the S3 root for datatools (`IncludeFile`) separately from artifact storage.
- **Example**:
  ```bash
  export METAFLOW_DATATOOLS_S3ROOT=s3://my-bucket/datatool-data
  ```

### `METAFLOW_DATATOOLS_AZUREROOT`
- **Type**: String
- **Default**: `<DATASTORE_SYSROOT_AZURE>/data` (auto-derived)
- **Use Case**: Azure root for `IncludeFile`. Internal use only — no public Azure datatools library is exposed.

### `METAFLOW_DATATOOLS_GSROOT`
- **Type**: String
- **Default**: `<DATASTORE_SYSROOT_GS>/data` (auto-derived)
- **Use Case**: GS root for `IncludeFile`. Internal use only — no public GS datatools library is exposed.

### `METAFLOW_DATATOOLS_LOCALROOT`
- **Type**: String (path)
- **Default**: `<DATASTORE_SYSROOT_LOCAL>/data` (auto-derived)
- **Use Case**: Local root used by `IncludeFile`.

### `METAFLOW_DATATOOLS_CLIENT_PARAMS`
- **Type**: JSON dictionary
- **Default**: `{}`
- **Note**: `endpoint_url` and `verify` are automatically injected here if `S3_ENDPOINT_URL` or `S3_VERIFY_CERTIFICATE` are set.

### `METAFLOW_DATATOOLS_SESSION_VARS`
- **Type**: JSON dictionary
- **Default**: `{}`
- **Use Case**: Session-level variables for datatools.

### `METAFLOW_TEMPDIR`
- **Type**: String (path)
- **Default**: `.` (current directory)
- **Use Case**: Temporary directory for intermediate files.
- **Example**:
  ```bash
  export METAFLOW_TEMPDIR=/tmp
  ```

### `METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT`
- **Type**: String (URL)
- **Default**: None
- **Use Case**: Azure Blob Storage service endpoint URL.
- **Example**:
  ```bash
  export METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT=https://myaccount.blob.core.windows.net
  ```

### `METAFLOW_AZURE_STORAGE_WORKLOAD_TYPE`
- **Type**: String
- **Default**: `general`
- **Allowed Values**: `general`, `high_throughput`
- **Use Case**: Switch Azure storage to process-based parallelism for large artifact workloads.
- **Example**:
  ```bash
  export METAFLOW_AZURE_STORAGE_WORKLOAD_TYPE=high_throughput
  ```

### `METAFLOW_GS_STORAGE_WORKLOAD_TYPE`
- **Type**: String
- **Default**: `general`
- **Allowed Values**: `general`, `high_throughput`
- **Use Case**: Switch GCS storage to process-based parallelism for large artifact workloads.
- **Example**:
  ```bash
  export METAFLOW_GS_STORAGE_WORKLOAD_TYPE=high_throughput
  ```

---

## Secrets Management

Configuration for accessing secrets from various backends.

### `METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION`
- **Type**: String (AWS region)
- **Default**: None
- **Use Case**: Default AWS region for Secrets Manager operations.
- **Example**:
  ```bash
  export METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION=us-west-2
  ```

### `METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_ROLE`
- **Type**: String (IAM role ARN)
- **Default**: None
- **Use Case**: IAM role to assume when accessing AWS Secrets Manager.
- **Example**:
  ```bash
  export METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_ROLE=arn:aws:iam::123456789:role/MetaflowSecretsRole
  ```

### `METAFLOW_GCP_SECRET_MANAGER_PREFIX`
- **Type**: String
- **Default**: None
- **Use Case**: Prefix for GCP secret resource names, so `@secret` decorator can use short names.
- **Note**: Whether the prefix ends with `/` affects the final secret name: `'projects/.../secrets/'` + `'mysecret'` → `'projects/.../secrets/mysecret'` vs `'projects/.../secrets/foo-'` + `'mysecret'` → `'projects/.../secrets/foo-mysecret'`.
- **Example**:
  ```bash
  export METAFLOW_GCP_SECRET_MANAGER_PREFIX=projects/1234567890/secrets/
  ```

### `METAFLOW_AZURE_KEY_VAULT_PREFIX`
- **Type**: String (URL prefix)
- **Default**: None
- **Use Case**: Prefix URL for Azure Key Vault — trailing slash is optional.
- **Example**:
  ```bash
  export METAFLOW_AZURE_KEY_VAULT_PREFIX=https://myvault.vault.azure.net/
  ```

---

## Metadata Service Configuration

Configuration for the centralized Metaflow metadata service.

### `METAFLOW_SERVICE_URL`
- **Type**: String (URL)
- **Default**: None
- **Use Case**: URL of the Metaflow metadata service.
- **Example**:
  ```bash
  export METAFLOW_SERVICE_URL=https://metaflow-service.example.com
  ```

### `METAFLOW_SERVICE_INTERNAL_URL`
- **Type**: String (URL)
- **Default**: Value of `SERVICE_URL` (same as external URL)
- **Use Case**: Internal URL for metadata service used by batch workers inside private networks.
- **Example**:
  ```bash
  export METAFLOW_SERVICE_INTERNAL_URL=http://metaflow-service.internal:8080
  ```

### `METAFLOW_SERVICE_AUTH_KEY`
- **Type**: String (API key)
- **Default**: None
- **Note**: Automatically injected into `SERVICE_HEADERS` as `x-api-key` when set.
- **Security**: Keep this secret!
- **Example**:
  ```bash
  export METAFLOW_SERVICE_AUTH_KEY=your-api-key
  ```

### `METAFLOW_SERVICE_HEADERS`
- **Type**: JSON dictionary
- **Default**: `{}`
- **Use Case**: Extra HTTP headers sent with all metadata service requests. `SERVICE_AUTH_KEY` automatically populates `x-api-key` here.
- **Example**:
  ```bash
  export METAFLOW_SERVICE_HEADERS='{"Authorization": "Bearer token"}'
  ```

### `METAFLOW_SERVICE_RETRY_COUNT`
- **Type**: Integer
- **Default**: `5`
- **Use Case**: Number of retries for metadata service requests.
- **Example**:
  ```bash
  export METAFLOW_SERVICE_RETRY_COUNT=3
  ```

### `METAFLOW_SERVICE_VERSION_CHECK`
- **Type**: Boolean
- **Default**: `True`
- **Use Case**: Disable only in development — version mismatch between client and service can cause subtle bugs.
- **Example**:
  ```bash
  export METAFLOW_SERVICE_VERSION_CHECK=False
  ```

### `METAFLOW_INCLUDE_FOREACH_STACK`
- **Type**: Boolean
- **Default**: `True`
- **Use Case**: Include foreach stack information in task metadata.
- **Example**:
  ```bash
  export METAFLOW_INCLUDE_FOREACH_STACK=False
  ```

### `METAFLOW_MAXIMUM_FOREACH_VALUE_CHARS`
- **Type**: Integer
- **Default**: `30`
- **Use Case**: Maximum length of the foreach value string stored in each `ForeachFrame`.
- **Example**:
  ```bash
  export METAFLOW_MAXIMUM_FOREACH_VALUE_CHARS=100
  ```

### `METAFLOW_DEFAULT_RUNTIME_LIMIT`
- **Type**: Integer (seconds)
- **Default**: `432000` (5 days)
- **Use Case**: Default maximum runtime for jobs launched by any compute provider (Batch, Kubernetes, etc.).
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_RUNTIME_LIMIT=86400  # 1 day
  ```

---

## Container & Execution Configuration

Configuration for container images and execution parameters.

### `METAFLOW_DEFAULT_CONTAINER_IMAGE`
- **Type**: String
- **Default**: None
- **Use Case**: Default container image used by Batch and Kubernetes.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_CONTAINER_IMAGE=python:3.10-slim
  ```

### `METAFLOW_DEFAULT_CONTAINER_REGISTRY`
- **Type**: String
- **Default**: None
- **Use Case**: Default container registry prefix.
- **Example**:
  ```bash
  export METAFLOW_DEFAULT_CONTAINER_REGISTRY=123456789.dkr.ecr.us-east-1.amazonaws.com
  ```

---

## Cards Configuration

Metaflow Cards is the built-in flow visualization and report system. These variables control where card data is stored across different backends.

### `METAFLOW_CARD_LOCALROOT`
- **Type**: String (path)
- **Default**: None
- **Use Case**: Local filesystem path for storing card data.
- **Example**:
  ```bash
  export METAFLOW_CARD_LOCALROOT=/tmp/metaflow_cards
  ```

### `METAFLOW_CARD_S3ROOT`
- **Type**: String
- **Default**: `<DATASTORE_SYSROOT_S3>/mf.cards` (auto-derived)
- **Use Case**: Override the S3 path for card storage.
- **Example**:
  ```bash
  export METAFLOW_CARD_S3ROOT=s3://my-bucket/metaflow-cards
  ```

### `METAFLOW_CARD_AZUREROOT`
- **Type**: String
- **Default**: `<DATASTORE_SYSROOT_AZURE>/mf.cards` (auto-derived)
- **Use Case**: Override the Azure path for card storage.

### `METAFLOW_CARD_GSROOT`
- **Type**: String
- **Default**: `<DATASTORE_SYSROOT_GS>/mf.cards` (auto-derived)
- **Use Case**: Override the GCS path for card storage.

### `METAFLOW_CARD_NO_WARNING`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Suppress warnings when no card is found.
- **Example**:
  ```bash
  export METAFLOW_CARD_NO_WARNING=True
  ```

### `METAFLOW_RUNTIME_CARD_RENDER_INTERVAL`
- **Type**: Integer (seconds)
- **Default**: `60`
- **Use Case**: How often cards are re-rendered during a running step.
- **Example**:
  ```bash
  export METAFLOW_RUNTIME_CARD_RENDER_INTERVAL=30
  ```

---

## AWS Batch Configuration

Configuration for AWS Batch compute backend.

### `METAFLOW_ECS_S3_ACCESS_IAM_ROLE`
- **Type**: String (IAM role ARN)
- **Default**: None (required if using AWS Batch)
- **Use Case**: IAM role for AWS Batch container with S3 (and optionally DynamoDB for Step Functions) access.
- **Example**:
  ```bash
  export METAFLOW_ECS_S3_ACCESS_IAM_ROLE=arn:aws:iam::123456789:role/MetaflowBatchRole
  ```

### `METAFLOW_ECS_FARGATE_EXECUTION_ROLE`
- **Type**: String (IAM role ARN)
- **Default**: None
- **Use Case**: IAM execution role for AWS Fargate tasks.
- **Example**:
  ```bash
  export METAFLOW_ECS_FARGATE_EXECUTION_ROLE=arn:aws:iam::123456789:role/FargateExecutionRole
  ```

### `METAFLOW_BATCH_JOB_QUEUE`
- **Type**: String (queue name or ARN)
- **Default**: None (required if using AWS Batch)
- **Use Case**: Default AWS Batch job queue name.
- **Example**:
  ```bash
  export METAFLOW_BATCH_JOB_QUEUE=metaflow-queue
  ```

### `METAFLOW_BATCH_CONTAINER_IMAGE`
- **Type**: String (image URI)
- **Default**: Value of `DEFAULT_CONTAINER_IMAGE`
- **Use Case**: Container image for Batch jobs (overrides the global default for Batch only).
- **Example**:
  ```bash
  export METAFLOW_BATCH_CONTAINER_IMAGE=python:3.10-slim
  ```

### `METAFLOW_BATCH_CONTAINER_REGISTRY`
- **Type**: String (registry URL)
- **Default**: Value of `DEFAULT_CONTAINER_REGISTRY`
- **Use Case**: Container registry for Batch jobs.

### `METAFLOW_BATCH_EMIT_TAGS`
- **Type**: Boolean
- **Default**: `False`
- **Note**: Requires `Batch:TagResource` permission — disabled by default because many deployments don't have it.
- **Example**:
  ```bash
  export METAFLOW_BATCH_EMIT_TAGS=True
  ```

### `METAFLOW_BATCH_DEFAULT_TAGS`
- **Type**: JSON dictionary
- **Default**: `{}`
- **Use Case**: Tags added to every Batch job in addition to the automatic defaults when `BATCH_EMIT_TAGS` is `True`.
- **Example**:
  ```bash
  export METAFLOW_BATCH_DEFAULT_TAGS='{"team": "ml", "project": "forecasting"}'
  ```

---

## AWS Step Functions Configuration

Configuration for AWS Step Functions deployment backend.

### `METAFLOW_SFN_IAM_ROLE`
- **Type**: String (IAM role ARN)
- **Default**: None (required for Step Functions)
- **Use Case**: IAM role for Step Functions with Batch and DynamoDB access.
- **Reference**: https://docs.aws.amazon.com/step-functions/latest/dg/batch-iam.html
- **Example**:
  ```bash
  export METAFLOW_SFN_IAM_ROLE=arn:aws:iam::123456789:role/MetaflowSFNRole
  ```

### `METAFLOW_SFN_DYNAMO_DB_TABLE`
- **Type**: String (table name)
- **Default**: None
- **Use Case**: DynamoDB table name. Partition key must be `pathspec` of type String.
- **Example**:
  ```bash
  export METAFLOW_SFN_DYNAMO_DB_TABLE=metaflow-sfn-state
  ```

### `METAFLOW_EVENTS_SFN_ACCESS_IAM_ROLE`
- **Type**: String (IAM role ARN)
- **Default**: None
- **Use Case**: IAM role for EventBridge to trigger Step Functions.
- **Reference**: https://docs.aws.amazon.com/eventbridge/latest/userguide/auth-and-access-control-eventbridge.html
- **Example**:
  ```bash
  export METAFLOW_EVENTS_SFN_ACCESS_IAM_ROLE=arn:aws:iam::123456789:role/MetaflowEventsRole
  ```

### `METAFLOW_SFN_STATE_MACHINE_PREFIX`
- **Type**: String
- **Default**: None
- **Note**: Automatically set to `AWS_SANDBOX_STACK_NAME` when sandbox is enabled.
- **Example**:
  ```bash
  export METAFLOW_SFN_STATE_MACHINE_PREFIX=prod
  ```

### `METAFLOW_SFN_EXECUTION_LOG_GROUP_ARN`
- **Type**: String (CloudWatch Log Group ARN)
- **Default**: None
- **Use Case**: CloudWatch Log Group ARN for Step Functions execution logs. Required when using `step-functions create --log-execution-history`.
- **Example**:
  ```bash
  export METAFLOW_SFN_EXECUTION_LOG_GROUP_ARN=arn:aws:logs:us-east-1:123456789:log-group:/aws/sfn/metaflow
  ```

### `METAFLOW_SFN_S3_DISTRIBUTED_MAP_OUTPUT_PATH`
- **Type**: String (s3://)
- **Default**: `<DATASTORE_SYSROOT_S3>/sfn_distributed_map_output` (auto-derived)
- **Use Case**: S3 path for storing AWS Step Functions Distributed Map results.

### `METAFLOW_SFN_COMPRESS_STATE_MACHINE`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Offload the step command from the Step Functions payload to S3 (useful for very large state machines).
- **Example**:
  ```bash
  export METAFLOW_SFN_COMPRESS_STATE_MACHINE=True
  ```

---

## Kubernetes Configuration

Comprehensive configuration for Kubernetes deployment backend.

### `METAFLOW_KUBERNETES_NAMESPACE`
- **Type**: String (k8s namespace)
- **Default**: `default`
- **Use Case**: Kubernetes namespace for all Metaflow objects.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_NAMESPACE=metaflow
  ```

### `METAFLOW_KUBERNETES_SERVICE_ACCOUNT`
- **Type**: String (service account name)
- **Default**: None
- **Use Case**: Default Kubernetes service account for Metaflow jobs.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_SERVICE_ACCOUNT=metaflow-sa
  ```

### `METAFLOW_KUBERNETES_NODE_SELECTOR`
- **Type**: String (comma-separated key=value)
- **Default**: `""` (empty)
- **Use Case**: Node selectors for job scheduling.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_NODE_SELECTOR=nodegroup=ml,instance-type=gpu
  ```

### `METAFLOW_KUBERNETES_TOLERATIONS`
- **Type**: String
- **Default**: `""` (empty)
- **Use Case**: Kubernetes tolerations for Metaflow pods.

### `METAFLOW_KUBERNETES_PERSISTENT_VOLUME_CLAIMS`
- **Type**: String (comma-separated)
- **Default**: `""` (empty)
- **Use Case**: Persistent volume claims to attach to pods.

### `METAFLOW_KUBERNETES_SECRETS`
- **Type**: String (comma-separated)
- **Default**: `""` (empty)
- **Use Case**: Kubernetes secrets to expose to pods.

### `METAFLOW_KUBERNETES_LABELS`
- **Type**: String (comma-separated key=value)
- **Default**: `""` (empty)
- **Use Case**: Default labels for Kubernetes pods.

### `METAFLOW_KUBERNETES_ANNOTATIONS`
- **Type**: String (key=value pairs)
- **Default**: `""` (empty)
- **Use Case**: Default annotations for Kubernetes pods.

### `METAFLOW_KUBERNETES_GPU_VENDOR`
- **Type**: String
- **Default**: `nvidia`
- **Allowed Values**: `nvidia`, `amd`
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_GPU_VENDOR=amd
  ```

### `METAFLOW_KUBERNETES_CPU`
- **Type**: String or Integer
- **Default**: None
- **Use Case**: Default CPU resource request for K8s jobs (e.g. `"500m"` or `2`).
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_CPU=2
  ```

### `METAFLOW_KUBERNETES_MEMORY`
- **Type**: Integer (MB)
- **Default**: None
- **Use Case**: Default memory request in MB.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_MEMORY=4096
  ```

### `METAFLOW_KUBERNETES_DISK`
- **Type**: Integer (MB)
- **Default**: None
- **Use Case**: Default ephemeral disk request in MB.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_DISK=10000
  ```

### `METAFLOW_KUBERNETES_SHARED_MEMORY`
- **Type**: Integer (MB)
- **Default**: None
- **Use Case**: Shared memory (`/dev/shm`) size in MB.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_SHARED_MEMORY=1024
  ```

### `METAFLOW_KUBERNETES_PORT`
- **Type**: Integer (port number)
- **Default**: None
- **Use Case**: Default port to open on pods.

### `METAFLOW_KUBERNETES_QOS`
- **Type**: String
- **Default**: `burstable`
- **Allowed Values**: `burstable`, `guaranteed`, `besteffort`
- **Use Case**: Control pod eviction priority.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_QOS=guaranteed
  ```

### `METAFLOW_KUBERNETES_CONTAINER_IMAGE`
- **Type**: String (image URI)
- **Default**: Value of `DEFAULT_CONTAINER_IMAGE`
- **Use Case**: Container image for K8s jobs.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_CONTAINER_IMAGE=python:3.10-slim
  ```

### `METAFLOW_KUBERNETES_CONTAINER_REGISTRY`
- **Type**: String (registry URL)
- **Default**: Value of `DEFAULT_CONTAINER_REGISTRY`
- **Use Case**: Container registry for K8s jobs.

### `METAFLOW_KUBERNETES_IMAGE_PULL_POLICY`
- **Type**: String
- **Default**: None (uses cluster default)
- **Allowed Values**: `Always`, `IfNotPresent`, `Never`
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_IMAGE_PULL_POLICY=Always
  ```

### `METAFLOW_KUBERNETES_IMAGE_PULL_SECRETS`
- **Type**: String (comma-separated)
- **Default**: `""` (empty)
- **Use Case**: Image pull secrets for private container registries.

### `METAFLOW_KUBERNETES_FETCH_EC2_METADATA`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Attempt to fetch EC2 instance metadata from within Kubernetes pods (useful on AWS EKS).
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_FETCH_EC2_METADATA=True
  ```

### `METAFLOW_KUBERNETES_CONDA_ARCH`
- **Type**: String (architecture)
- **Default**: None
- **Use Case**: CPU architecture of Kubernetes nodes, used for `@conda`/`@pypi` package resolution.
- **Example**:
  ```bash
  export METAFLOW_KUBERNETES_CONDA_ARCH=aarch64
  ```

### `METAFLOW_KUBERNETES_JOBSET_GROUP`
- **Type**: String (API group)
- **Default**: `jobset.x-k8s.io`
- **Use Case**: API group for Kubernetes JobSet resource.

### `METAFLOW_KUBERNETES_JOBSET_VERSION`
- **Type**: String (API version)
- **Default**: `v1alpha2`
- **Use Case**: API version for Kubernetes JobSet resource.

### `METAFLOW_KUBERNETES_JOB_TERMINATE_MODE`
- **Type**: String
- **Default**: `stop`
- **Use Case**: How to terminate Kubernetes jobs on flow cancel.

---

## Argo Workflows Configuration

Configuration for Argo Workflows integration.

### `METAFLOW_ARGO_WORKFLOWS_KUBERNETES_SECRETS`
- **Type**: String (comma-separated)
- **Default**: `""` (empty)
- **Use Case**: Kubernetes secrets to expose to Argo Workflow pods.

### `METAFLOW_ARGO_WORKFLOWS_ENV_VARS_TO_SKIP`
- **Type**: String (comma-separated)
- **Default**: `""` (empty)
- **Use Case**: Environment variables to exclude from Argo Workflow pod specs.
- **Example**:
  ```bash
  export METAFLOW_ARGO_WORKFLOWS_ENV_VARS_TO_SKIP=AWS_SECRET_ACCESS_KEY,GITHUB_TOKEN
  ```

### `METAFLOW_ARGO_WORKFLOWS_CAPTURE_ERROR_SCRIPT`
- **Type**: String (script content)
- **Default**: None
- **Use Case**: Script run on Argo pod error to capture additional error details.

### `METAFLOW_ARGO_WORKFLOWS_UI_URL`
- **Type**: String (URL)
- **Default**: None
- **Use Case**: URL for the Argo Workflows UI, used for display links in CLI output.
- **Example**:
  ```bash
  export METAFLOW_ARGO_WORKFLOWS_UI_URL=https://argo.mycompany.com
  ```

---

## Argo Events Configuration

Configuration for Argo Events (event-driven workflows).

### `METAFLOW_ARGO_EVENTS_SERVICE_ACCOUNT`
- **Type**: String (service account name)
- **Default**: None
- **Use Case**: Kubernetes service account for Argo Events resources.

### `METAFLOW_ARGO_EVENTS_EVENT_BUS`
- **Type**: String (event bus name)
- **Default**: `default`
- **Use Case**: Name of the Argo Events EventBus.

### `METAFLOW_ARGO_EVENTS_EVENT_SOURCE`
- **Type**: String (event source name)
- **Default**: None
- **Use Case**: Argo Events EventSource name.

### `METAFLOW_ARGO_EVENTS_EVENT`
- **Type**: String (event name)
- **Default**: None
- **Use Case**: Event name in the EventSource.

### `METAFLOW_ARGO_EVENTS_WEBHOOK_URL`
- **Type**: String (URL)
- **Default**: None
- **Use Case**: External webhook URL for Argo Events triggers.
- **Example**:
  ```bash
  export METAFLOW_ARGO_EVENTS_WEBHOOK_URL=https://argo-events.example.com/webhook
  ```

### `METAFLOW_ARGO_EVENTS_INTERNAL_WEBHOOK_URL`
- **Type**: String (URL)
- **Default**: Value of `ARGO_EVENTS_WEBHOOK_URL`
- **Use Case**: Internal webhook URL used within the cluster.

### `METAFLOW_ARGO_EVENTS_WEBHOOK_AUTH`
- **Type**: String
- **Default**: `none`
- **Allowed Values**: `none`, `token`
- **Use Case**: Webhook authentication method.

### `METAFLOW_ARGO_EVENTS_SENSOR_NAMESPACE`
- **Type**: String (k8s namespace)
- **Default**: Value of `KUBERNETES_NAMESPACE`
- **Use Case**: Namespace for Argo Sensor objects.

### `METAFLOW_NAMESPACED_EVENTS_PREFIX`
- **Type**: String
- **Default**: `mfns`
- **Use Case**: Prefix for namespaced events used by `@trigger` with `namespaced=True`.

---

## Airflow Configuration

Configuration for Airflow integration.

### `METAFLOW_AIRFLOW_KUBERNETES_STARTUP_TIMEOUT_SECONDS`
- **Type**: Integer (seconds)
- **Default**: `3600` (1 hour)
- **Use Case**: Sets `startup_timeout_seconds` in Airflow's `KubernetesPodOperator`.
- **Example**:
  ```bash
  export METAFLOW_AIRFLOW_KUBERNETES_STARTUP_TIMEOUT_SECONDS=1800
  ```

### `METAFLOW_AIRFLOW_KUBERNETES_CONN_ID`
- **Type**: String (Airflow connection ID)
- **Default**: None
- **Use Case**: Sets `kubernetes_conn_id` in Airflow's `KubernetesPodOperator`.
- **Example**:
  ```bash
  export METAFLOW_AIRFLOW_KUBERNETES_CONN_ID=kubernetes_default
  ```

### `METAFLOW_AIRFLOW_KUBERNETES_KUBECONFIG_FILE`
- **Type**: String (file path)
- **Default**: None
- **Use Case**: Path to kubeconfig file for Airflow Kubernetes operator.

### `METAFLOW_AIRFLOW_KUBERNETES_KUBECONFIG_CONTEXT`
- **Type**: String (context name)
- **Default**: None
- **Use Case**: Kubeconfig context to use for Airflow Kubernetes operator.

---

## Conda Configuration

Configuration for Conda environment management.

### `METAFLOW_CONDA_PACKAGE_S3ROOT`
- **Type**: String (S3 path)
- **Default**: None
- **Use Case**: S3 root for caching conda packages.
- **Example**:
  ```bash
  export METAFLOW_CONDA_PACKAGE_S3ROOT=s3://my-bucket/conda-packages
  ```

### `METAFLOW_CONDA_PACKAGE_AZUREROOT`
- **Type**: String
- **Default**: None
- **Use Case**: Azure root for caching conda packages.

### `METAFLOW_CONDA_PACKAGE_GSROOT`
- **Type**: String
- **Default**: None
- **Use Case**: GCS root for caching conda packages.

### `METAFLOW_CONDA_DEPENDENCY_RESOLVER`
- **Type**: String
- **Default**: `conda`
- **Allowed Values**: `conda`, `mamba`
- **Use Case**: Use Mamba for faster dependency resolution.
- **Example**:
  ```bash
  export METAFLOW_CONDA_DEPENDENCY_RESOLVER=mamba
  ```

### `METAFLOW_CONDA_USE_FAST_INIT`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Enable the fast conda initialization binary for quicker environment startup.
- **Example**:
  ```bash
  export METAFLOW_CONDA_USE_FAST_INIT=True
  ```

---

## Spin Configuration

Configuration for Spin (workflow execution framework).

### `METAFLOW_SPIN_ALLOWED_DECORATORS`
- **Type**: List (JSON array)
- **Default**: `["conda", "pypi", "conda_base", "pypi_base", "environment", "project", "timeout", "conda_env_internal", "card"]`
- **Use Case**: Whitelist of decorators permitted on Spin steps.
- **Note**: The default value is a Python list. When setting via environment variable, provide valid JSON array syntax.
- **Example**:
  ```bash
  export METAFLOW_SPIN_ALLOWED_DECORATORS='["conda", "pypi", "timeout"]'
  ```

### `METAFLOW_SPIN_DISALLOWED_DECORATORS`
- **Type**: List (JSON array)
- **Default**: `["parallel"]`
- **Use Case**: Blacklist of decorators not permitted on Spin steps. Decorators not in either list are silently ignored.

### `METAFLOW_SPIN_PERSIST`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Persist Spin environments between runs.
- **Example**:
  ```bash
  export METAFLOW_SPIN_PERSIST=True
  ```

---

## Debugging & Profiling

Debug flags follow the pattern `METAFLOW_DEBUG_<OPTION>`. All default to `False`.

| Variable | Default | Notes |
|---|---|---|
| `METAFLOW_DEBUG_SUBCOMMAND` | `False` | Debug subcommand execution |
| `METAFLOW_DEBUG_SIDECAR` | `False` | Debug sidecar process communication |
| `METAFLOW_DEBUG_S3CLIENT` | `False` | Debug S3 client operations — enable to troubleshoot storage issues |
| `METAFLOW_DEBUG_TRACING` | `False` | Debug distributed tracing subsystem |
| `METAFLOW_DEBUG_STUBGEN` | `False` | Debug stub generation |
| `METAFLOW_DEBUG_USERCONF` | `False` | Debug user configuration loading — enable to see how config.json is parsed and merged |
| `METAFLOW_DEBUG_CONDA` | `False` | Debug conda environment operations |
| `METAFLOW_DEBUG_PACKAGE` | `False` | Debug code package operations |

**Example**:
```bash
export METAFLOW_DEBUG_S3CLIENT=True
export METAFLOW_DEBUG_TRACING=True
```

### `METAFLOW_PROFILE_FROM_START`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Enable Python profiling from flow start.
- **Example**:
  ```bash
  export METAFLOW_PROFILE_FROM_START=True
  ```

### `METAFLOW_ESCAPE_HATCH_WARNING`
- **Type**: Boolean
- **Default**: `True`
- **Use Case**: Show a warning when packages are not locked with an escape hatch. Set to `False` once packages are tested and stable.
- **Example**:
  ```bash
  export METAFLOW_ESCAPE_HATCH_WARNING=False
  ```

---

## AWS Sandbox Configuration

> ⚠️ **Sandbox side-effects:** When `METAFLOW_AWS_SANDBOX_ENABLED=True`, Metaflow automatically: (1) sets `os.environ["AWS_DEFAULT_REGION"]` to `AWS_SANDBOX_REGION`, (2) overwrites `SERVICE_INTERNAL_URL` with `AWS_SANDBOX_INTERNAL_SERVICE_URL`, and (3) injects the API key into `SERVICE_HEADERS` as `x-api-key`. These side-effects are not visible from individual variable settings alone.

### `METAFLOW_AWS_SANDBOX_ENABLED`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Enable the managed Metaflow AWS sandbox.
- **Example**:
  ```bash
  export METAFLOW_AWS_SANDBOX_ENABLED=True
  ```

### `METAFLOW_AWS_SANDBOX_API_KEY`
- **Type**: String (API key)
- **Default**: None (required when sandbox enabled)
- **Security**: Keep secret — do not commit to version control.
- **Example**:
  ```bash
  export METAFLOW_AWS_SANDBOX_API_KEY=your-sandbox-api-key
  ```

### `METAFLOW_AWS_SANDBOX_INTERNAL_SERVICE_URL`
- **Type**: String (URL)
- **Default**: None
- **Note**: When sandbox is enabled, this value is automatically applied to `SERVICE_INTERNAL_URL`.

### `METAFLOW_AWS_SANDBOX_REGION`
- **Type**: String (AWS region)
- **Default**: None
- **Note**: When sandbox is enabled, this is automatically written to `os.environ["AWS_DEFAULT_REGION"]`.
- **Example**:
  ```bash
  export METAFLOW_AWS_SANDBOX_REGION=us-west-2
  ```

### `METAFLOW_AWS_SANDBOX_STACK_NAME`
- **Type**: String
- **Default**: None
- **Note**: When sandbox is enabled, this is automatically used as `SFN_STATE_MACHINE_PREFIX`.

### `METAFLOW_KUBERNETES_SANDBOX_INIT_SCRIPT`
- **Type**: String
- **Default**: None
- **Use Case**: Initialization script content or path for the Kubernetes sandbox environment.

---

## Tracing Configuration

Configuration for distributed tracing and observability.

### `METAFLOW_OTEL_ENDPOINT`
- **Type**: String (URL)
- **Default**: None
- **Use Case**: OpenTelemetry collector endpoint for distributed tracing.
- **Example**:
  ```bash
  export METAFLOW_OTEL_ENDPOINT=http://otel-collector.example.com:4317
  ```

### `METAFLOW_ZIPKIN_ENDPOINT`
- **Type**: String (URL)
- **Default**: None
- **Use Case**: Zipkin backend endpoint.
- **Example**:
  ```bash
  export METAFLOW_ZIPKIN_ENDPOINT=http://zipkin.example.com:9411
  ```

### `METAFLOW_CONSOLE_TRACE_ENABLED`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Print trace output to the console — useful in development.
- **Example**:
  ```bash
  export METAFLOW_CONSOLE_TRACE_ENABLED=True
  ```

### `METAFLOW_DISABLE_TRACING`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Prevents the tracing module from loading during Conda bootstrapping.
- **Note**: ⚠️ This variable bypasses the `from_conf()` config system and reads directly from `os.environ`. It **cannot** be set via `config.json` — it must be a real shell environment variable.
- **Example**:
  ```bash
  export METAFLOW_DISABLE_TRACING=True
  ```

---

## Feature Flags & Advanced Options

Configuration for experimental features and advanced options.

### `METAFLOW_FEAT_ALWAYS_UPLOAD_CODE_PACKAGE`
- **Type**: Boolean
- **Default**: `False`
- **Use Case**: Always upload a fresh code package even when a cached version exists.
- **Example**:
  ```bash
  export METAFLOW_FEAT_ALWAYS_UPLOAD_CODE_PACKAGE=True
  ```

### `METAFLOW_CLICK_API_PROCESS_CONFIG`
- **Type**: Boolean
- **Default**: `True`
- **Use Case**: Process configs when using the Click API for Runner/Deployer. Set to `False` to disable config processing in that context.
- **Example**:
  ```bash
  export METAFLOW_CLICK_API_PROCESS_CONFIG=False
  ```

---

## Plugin Configuration

Metaflow supports plugin filtering via `ENABLED_<CATEGORY>` environment variables, where `<CATEGORY>` is one of the following:

- `STEP_DECORATOR`
- `FLOW_DECORATOR`
- `ENVIRONMENT`
- `METADATA_PROVIDER`
- `DATASTORE`
- `SIDECAR`
- `CLI`

**Example:**
```bash
ENABLED_STEP_DECORATOR='["batch","resources"]'
```

---

## Hidden / Advanced Variables

These variables exist in Metaflow but are rarely documented. Use with caution — behavior may change between releases.

| Variable | Default |
|---|---|
| `FEAT_ALWAYS_UPLOAD_CODE_PACKAGE` | `False` |
| `CLICK_API_PROCESS_CONFIG` | `True` |
| `KUBERNETES_SANDBOX_INIT_SCRIPT` | `None` |
| `OTEL_ENDPOINT` | `None` |
| `ZIPKIN_ENDPOINT` | `None` |
| `CONSOLE_TRACE_ENABLED` | `False` |

---

## Configuration File Format

Environment variables can also be set in a `.metaflow/config.json` file in your project directory.

> **Key naming:** Keys inside `config.json` omit the `METAFLOW_` prefix. The variable `METAFLOW_DEFAULT_DATASTORE` is set with the key `"DEFAULT_DATASTORE"` in the JSON file.

```json
{
  "DEFAULT_DATASTORE": "s3",
  "DATASTORE_SYSROOT_S3": "s3://my-bucket/metaflow",
  "DEFAULT_ENVIRONMENT": "local",
  "KUBERNETES_NAMESPACE": "metaflow",
  "KUBERNETES_CPU": 2,
  "KUBERNETES_MEMORY": 4096,
  "SERVICE_URL": "https://metaflow-service.example.com"
}
```

---

## Best Practices

### Security
- **Never commit sensitive variables** (`AWS_SANDBOX_API_KEY`, IAM roles, auth keys) to version control
- Use environment variables or secure vaults (HashiCorp Vault, AWS Secrets Manager) for secrets
- Rotate API keys regularly
- Use IAM roles instead of access keys wherever possible

### Performance
- Tune `S3_WORKER_COUNT` based on your network bandwidth and artifact sizes
- Use `CONDA_USE_FAST_INIT=True` and `CONDA_DEPENDENCY_RESOLVER=mamba` for faster environment initialization
- Set `AZURE_STORAGE_WORKLOAD_TYPE=high_throughput` or `GS_STORAGE_WORKLOAD_TYPE=high_throughput` for large artifact workloads
- Set appropriate Kubernetes CPU/memory requests to avoid resource contention

### Debugging
- Enable `DEBUG_S3CLIENT` to troubleshoot S3 storage issues
- Enable `DEBUG_USERCONF` to trace how `config.json` is loaded and merged
- Use `DEBUG_TRACING` for distributed tracing problems
- Enable `CONSOLE_TRACE_ENABLED` in development environments
- Set `SERVICE_VERSION_CHECK=False` only in development — mismatch can cause silent bugs in production

### Multi-Environment Setup
- Use separate `config.json` files per environment (dev/staging/production)
- Store environment-specific configs under `.metaflow/` and select with shell scripts or CI/CD tools
- Use `SFN_STATE_MACHINE_PREFIX` to namespace Step Functions state machines per environment
- Tag Batch and Kubernetes resources with environment labels using `DEFAULT_TAGS` variables

---

## Troubleshooting

### Variable Not Taking Effect
- Check resolution order: shell env > `config.json` > default. A shell env var always wins.
- Remember: `config.json` keys omit `METAFLOW_` prefix.
- Enable `METAFLOW_DEBUG_USERCONF=True` to trace config loading.
- Exception: `METAFLOW_DISABLE_TRACING` must be a shell env var — it bypasses `config.json` entirely.

### Connection Issues
- Check `SERVICE_URL` and `SERVICE_INTERNAL_URL` connectivity
- Verify `S3_ENDPOINT_URL` for custom/self-hosted S3-compatible services
- Ensure service ports are accessible from the job execution environment

### Performance Issues
- Increase `S3_WORKER_COUNT` for slow S3 upload/download
- Enable `S3_LOG_TRANSIENT_RETRIES` to identify throttling
- Adjust `KUBERNETES_CPU` and `KUBERNETES_MEMORY` for resource constraints
- Use `AZURE_STORAGE_WORKLOAD_TYPE=high_throughput` for large artifact workloads

### Sandbox Issues
- Verify `METAFLOW_AWS_SANDBOX_ENABLED=True`, `REGION`, `API_KEY`, and `INTERNAL_SERVICE_URL` are all set
- Remember that enabling sandbox silently overwrites `SERVICE_INTERNAL_URL` and injects into `SERVICE_HEADERS` — check these values if requests are failing

---

## See Also

- [Metaflow Documentation](https://docs.metaflow.org)
- [Configuration System](https://docs.metaflow.org/metaflow/configuration)
- [AWS Batch Integration](https://docs.metaflow.org/metaflow/aws-batch)
- [Kubernetes Integration](https://docs.metaflow.org/metaflow/kubernetes)
- [Metadata Service](https://docs.metaflow.org/metaflow/metadata)
- [Metaflow Cards](https://docs.metaflow.org/metaflow/visualizing-results)