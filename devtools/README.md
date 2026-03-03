# Metaflow Devstack

A local Kubernetes development environment for Metaflow, built on [Minikube](https://minikube.sigs.k8s.io/) and [Tilt](https://tilt.dev/).

## Prerequisites

- **Docker** (Docker Desktop, OrbStack, Colima, or Rancher Desktop)
- **make**

Everything else (Minikube, Tilt, Helm, gum) is installed automatically on first run.

## Quickstart

```bash
cd devtools
make up        # interactive service picker, then starts the stack
make shell     # open a shell with Metaflow config pre-loaded
```

To start everything without the picker:

```bash
make all-up
```

To start a specific subset of services:

```bash
SERVICES_OVERRIDE=corral,minio make up
```

## Services

| Service | Description | Depends on | Host port(s) |
|---|---|---|---|
| `minio` | S3-compatible object storage | — | 9000 (API), 9001 (Console) |
| `postgresql` | PostgreSQL database | — | 5432 |
| `metadata-service` | Metaflow metadata API | postgresql | 8080 |
| `ui` | Metaflow UI | postgresql, minio | 3000 |
| `argo-workflows` | Argo Workflows controller + server | — | 2746 |
| `argo-events` | Argo Events controller + webhook | argo-workflows | 12000 |
| `jobset` | Kubernetes JobSet controller | — | — |
| `corral` | Local AWS Batch emulator | minio | 8000 |
| `ddb-local` | DynamoDB Local | — | 8765 |
| `sfn-local` | AWS Step Functions Local | ddb-local | 8082 |
| `azurite` | Azure Blob / Queue / Table emulator | — | 10000–10002 |
| `fake-gcs-server` | Google Cloud Storage emulator | — | 4443 |

Dependencies are resolved automatically — selecting `sfn-local` in the picker also starts `ddb-local`.

## Development shell

`make shell` waits for the stack to be ready and then opens a sub-shell with Metaflow pointed at the local stack:

```
METAFLOW_HOME=.devtools
METAFLOW_PROFILE=local      # loads .devtools/config_local.json
AWS_CONFIG_FILE=.devtools/aws_config   # MinIO credentials (if minio is running)
```

For Azure or GCS datastores, also source the extra env file:

```bash
source .devtools/env_local  # sets AZURE_STORAGE_CONNECTION_STRING, STORAGE_EMULATOR_HOST
```

Then run flows normally:

```bash
python myflow.py run
```

## Credentials

| Service | Credential |
|---|---|
| MinIO | `rootuser` / `rootpass123` |
| PostgreSQL | `metaflow` / `metaflow123` / db `metaflow` |
| DynamoDB Local / SFN Local / corral | any value (no auth) |
| Azurite | account `devstoreaccount1`, key in `.devtools/env_local` |
| fake-gcs-server | no auth required |

## Makefile targets

| Target | Description |
|---|---|
| `make up` | Interactive picker → start selected services |
| `make all-up` | Start all services (skips picker) |
| `make shell` | Open Metaflow-configured dev shell |
| `make down` | Tear down Minikube cluster and clean up |
| `make dashboard` | Open Minikube dashboard |
| `make ui` | Wait for Metaflow UI and open it in a browser |
| `make tunnel` | Run `minikube tunnel` (called automatically by `up`) |

## Teardown

```bash
make down
```

This stops Tilt, deletes the Minikube cluster, and removes all generated files under `.devtools/`.
