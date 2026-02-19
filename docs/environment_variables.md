\# Environment Variables



Metaflow configuration values can be overridden using environment variables.



All configuration variables defined in `metaflow/metaflow\_config.py`

can be overridden using the following naming convention:



```

METAFLOW\_<CONFIG\_NAME>

```



For example, the configuration:



```python

DEFAULT\_DATASTORE = from\_conf("DEFAULT\_DATASTORE", "local")

```



can be overridden with:



```bash

export METAFLOW\_DEFAULT\_DATASTORE=s3

```



Environment variables take precedence over configuration files.



---



\# Commonly Used Environment Variables



Below are commonly configured environment variables grouped by category.



---



\## User \& Runtime



\### METAFLOW\_USER



Overrides the detected username used for runs.  

Useful in CI environments or containers where the system user cannot be determined.



```bash

export METAFLOW\_USER=your\_username

```



---



\### METAFLOW\_DEFAULT\_ENVIRONMENT



Specifies the default execution environment.



Default: `local`



```bash

export METAFLOW\_DEFAULT\_ENVIRONMENT=local

```



---



\### METAFLOW\_DEFAULT\_DATASTORE



Specifies the default datastore backend (`local`, `s3`, `azure`, `gs`).



Default: `local`



```bash

export METAFLOW\_DEFAULT\_DATASTORE=s3

```



---



\## Metadata Service



\### METAFLOW\_SERVICE\_URL



Base URL for the Metaflow metadata service.



```bash

export METAFLOW\_SERVICE\_URL=https://metaflow.example.com

```



---



\### METAFLOW\_SERVICE\_AUTH\_KEY



Authentication key used when connecting to the metadata service.



```bash

export METAFLOW\_SERVICE\_AUTH\_KEY=<api\_key>

```



---



\## Datastore Configuration



\### METAFLOW\_DATASTORE\_SYSROOT\_LOCAL



Root directory for the local datastore.



---



\### METAFLOW\_DATASTORE\_SYSROOT\_S3



S3 root path used when running with the S3 datastore.



```bash

export METAFLOW\_DATASTORE\_SYSROOT\_S3=s3://my-bucket/metaflow

```



---



\### METAFLOW\_DATASTORE\_SYSROOT\_AZURE



Azure Blob Storage root path.



---



\### METAFLOW\_DATASTORE\_SYSROOT\_GS



Google Cloud Storage root path.



---



\## AWS Batch



\### METAFLOW\_BATCH\_JOB\_QUEUE



AWS Batch job queue used for execution.



```bash

export METAFLOW\_BATCH\_JOB\_QUEUE=my-queue

```



---



\### METAFLOW\_BATCH\_CONTAINER\_IMAGE



Default container image used for AWS Batch jobs.



---



\## Kubernetes



\### METAFLOW\_KUBERNETES\_NAMESPACE



Kubernetes namespace used for execution.



Default: `default`



```bash

export METAFLOW\_KUBERNETES\_NAMESPACE=ml-workflows

```



---



\### METAFLOW\_KUBERNETES\_CONTAINER\_IMAGE



Default container image used for Kubernetes jobs.



---



\## Secrets \& Security



\### METAFLOW\_DEFAULT\_SECRETS\_BACKEND\_TYPE



Specifies the default secrets backend.



---



\### METAFLOW\_AWS\_SECRETS\_MANAGER\_DEFAULT\_REGION



AWS region used for AWS Secrets Manager.



---



\## Debugging



Debug flags can be enabled using:



```

METAFLOW\_DEBUG\_<NAME>

```



Examples:



```bash

export METAFLOW\_DEBUG\_S3CLIENT=True

export METAFLOW\_DEBUG\_TRACING=True

```



---



\# Naming Rule (Important)



Every configuration defined as:



```python

SOME\_NAME = from\_conf("SOME\_NAME", default)

```



can be overridden using:



```

METAFLOW\_SOME\_NAME

```



Refer to `metaflow/metaflow\_config.py` for the authoritative

and complete list of supported configuration values.



---



\# Troubleshooting



\## "Unknown user" error



If you encounter an \*\*"unknown user"\*\* error:



```bash

export METAFLOW\_USER=your\_username

```



To list active Metaflow environment variables:



```bash

env | grep METAFLOW

```



