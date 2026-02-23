# Environment Variables

Metaflow configuration values can be overridden using environment variables.

All configuration variables defined in `metaflow/metaflow_config.py`
can be overridden using the following naming convention:
```
METAFLOW_<CONFIG_NAME>
```
For example, the configuration:
```python
DEFAULT_DATASTORE = from_conf("DEFAULT_DATASTORE", "local")
```
can be overridden with:
```bash
export METAFLOW_DEFAULT_DATASTORE=s3
```
Environment variables take precedence over configuration files,
which in turn override internal defaults.

---
## Commonly Used Environment Variables
Below are commonly configured environment variables grouped by category.


### User & Runtime
#### METAFLOW_USER
Overrides the detected username used for runs.  
Useful in CI environments or containers where the system user cannot be determined.
```bash
export METAFLOW_USER=your_username
```

#### METAFLOW_RUNTIME_NAME
Defines the runtime environment name associated with a run.
If not set, it defaults to `dev`.
Used by the metadata service to identify the runtime context of executions.
```bash
export METAFLOW_RUNTIME_NAME=prod
```
> Note: This variable is read directly from the environment and does not use the `from_conf()` configuration pattern.

#### METAFLOW_PRODUCTION_TOKEN
Defines a production token used for identifying or managing
production deployments.
This variable is used by deployment backends such as
AWS Step Functions, Argo Workflows, and Airflow.
```bash
export METAFLOW_PRODUCTION_TOKEN=<token>
```
> Note: This variable is read directly from the environment and does not use the `from_conf()` configuration pattern.


#### METAFLOW_DEFAULT_ENVIRONMENT
Specifies the default execution environment.
Valid values: `local`, `conda`, `pypi`, `uv`.
Default: `local`
```bash
export METAFLOW_DEFAULT_ENVIRONMENT=pypi
```


#### METAFLOW_DEFAULT_DATASTORE
Specifies the default datastore backend (`local`, `s3`, `azure`, `gs`).
Default: `local`
```bash
export METAFLOW_DEFAULT_DATASTORE=s3
```

### Metadata Service
#### METAFLOW_DEFAULT_METADATA
Specifies the metadata provider to use.
Valid values: `local`, `service`.
Default: `local`
```bash
export METAFLOW_DEFAULT_METADATA=service
```

#### METAFLOW_SERVICE_URL
Base URL for the Metaflow metadata service.
```bash
export METAFLOW_SERVICE_URL=https://metaflow.example.com
```

#### METAFLOW_SERVICE_AUTH_KEY
Authentication key used when connecting to the metadata service.
```bash
export METAFLOW_SERVICE_AUTH_KEY=<api_key>
```


### Datastore Configuration
#### METAFLOW_DATASTORE_SYSROOT_LOCAL
Root directory for the local datastore.


#### METAFLOW_DATASTORE_SYSROOT_S3
S3 root path used when running with the S3 datastore.
```bash
export METAFLOW_DATASTORE_SYSROOT_S3=s3://my-bucket/metaflow
```


#### METAFLOW_DATASTORE_SYSROOT_AZURE
Azure Blob Storage root path.


#### METAFLOW_DATASTORE_SYSROOT_GS
Google Cloud Storage root path.


### AWS Batch
#### METAFLOW_BATCH_JOB_QUEUE
AWS Batch job queue used for execution.
```bash
export METAFLOW_BATCH_JOB_QUEUE=my-queue
```

#### METAFLOW_BATCH_CONTAINER_IMAGE
Default container image used for AWS Batch jobs.


### Kubernetes
#### METAFLOW_KUBERNETES_NAMESPACE
Kubernetes namespace used for execution.
Default: `default`
```bash
export METAFLOW_KUBERNETES_NAMESPACE=ml-workflows
```

#### METAFLOW_KUBERNETES_CONTAINER_IMAGE
Default container image used for Kubernetes jobs.


### Secrets & Security
#### METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE
Specifies the default secrets backend.


#### METAFLOW_AWS_SECRETS_MANAGER_DEFAULT_REGION
AWS region used for AWS Secrets Manager.


### Debugging
Debug flags can be enabled using:
```
METAFLOW_DEBUG_<NAME>
```
Examples:
```bash
export METAFLOW_DEBUG_S3CLIENT=1
export METAFLOW_DEBUG_TRACING=1
```

## Troubleshooting
### "Unknown user" error
If you encounter an **"unknown user"** error:
```bash
export METAFLOW_USER=your_username
```

### Environment variable not taking effect
1. Ensure the variable is exported in your shell.
2. Restart your shell session if necessary.
3. Confirm it is set:
```bash
env | grep METAFLOW
```
4. Verify that the variable name matches the pattern:
```
METAFLOW_<CONFIG_NAME>
```

## Notes
- Environment variables override configuration files.
- Configuration files override internal defaults.
- Some runtime-specific variables (e.g., `METAFLOW_RUNTIME_NAME`, `METAFLOW_PRODUCTION_TOKEN`) are read directly from the environment and are not defined via `from_conf()` in `metaflow_config.py`.
- The complete and authoritative list of configuration values is defined in `metaflow/metaflow_config.py`.

