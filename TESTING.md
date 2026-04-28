# Testing Guide

## Setup

```bash
pip install -e ".[dev]"
pip install pre-commit && pre-commit install
```

> **Note — `METAFLOW_USER`:** tox sets this to `tester` automatically. If you
> run `pytest` directly on a host where `$USER` is `root`, export it first:
> `export METAFLOW_USER=tester`

---

## What to run

```
unit tests  →  core-local  →  open PR  →  CI handles the rest
```

| Suite | Command | Needs | Time |
|-------|---------|-------|------|
| Unit | `tox` | nothing | ~2 min |
| Core — local | `tox -c test/core/tox.ini -e core-local` | nothing | ~1 hr |
| Core — GCS | `tox -c test/core/tox.ini -e core-gcs` | devtools `fake-gcs-server` | ~1 hr |
| Core — Azure | `tox -c test/core/tox.ini -e core-azure` | devtools `azurite` | ~1 hr |
| Core — Batch/K8s/Argo/SFN | `tox -c test/core/tox.ini -e core-<name>` | full devstack | 2–3 hr |
| UX — local | `tox -e ux-local` | nothing | ~30 min |
| UX — cloud | `tox -e ux-<name>` | full devstack | ~1 hr |

Run **unit + core-local** before every PR. Cloud-backend tests (`core-gcs`,
`core-batch`, …) are only needed if you changed that backend's storage code —
otherwise let CI run them.

---

## Unit tests

```bash
tox                                         # all unit tests
pytest test/unit/test_datastore.py -v       # single file
pytest test/unit/ -k "artifact" -v          # keyword filter
```

Must pass on Python 3.7 – 3.14; CI runs the full matrix.

---

## Core integration tests

Each test generates a Metaflow flow from a graph topology (15 templates) ×
flow definition (~64 classes), runs it as a subprocess, and verifies results
in-process. This yields ~470 parametrised items per backend, identified as
`backend/graph/FlowDefinition/executor`.

### core-local

```bash
tox -c test/core/tox.ini -e core-local

# Filters (useful when iterating on a fix)
tox -c test/core/tox.ini -e core-local -- --core-tests BasicArtifact
tox -c test/core/tox.ini -e core-local -- --core-graphs simple-foreach
tox -c test/core/tox.ini -e core-local -- -n auto   # parallel
```

### core-gcs / core-azure (cloud storage emulators)

Only needed when you changed GCS or Azure storage code. The emulators are
part of the devtools stack:

```bash
cd devtools
SERVICES_OVERRIDE=fake-gcs-server make up   # GCS  (port 4443)
SERVICES_OVERRIDE=azurite make up           # Azure (port 10000)
```

Then:

```bash
tox -c test/core/tox.ini -e core-gcs
tox -c test/core/tox.ini -e core-azure
```

### core-batch / core-k8s / core-argo / core-sfn

These require the full devstack. For most PRs, **let CI run them**. If you
need to debug a scheduler-specific failure locally, start only the services
that backend needs, then run the env:

```bash
# Required services per backend:
#   core-batch: minio, postgresql, metadata-service, localbatch
#   core-k8s:   minio, postgresql, metadata-service
#   core-argo:  minio, postgresql, metadata-service, argo-workflows
#   core-sfn:   minio, postgresql, metadata-service, localbatch, ddb-local, sfn-local
cd devtools && SERVICES_OVERRIDE=minio,postgresql,metadata-service,localbatch make up
```

See `devtools/README.md` for the full devstack reference.

---

## Code style

```bash
black .                       # format
pre-commit run --all-files    # all checks
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `tox: command not found` | `pip install tox` |
| `Username 'root' is not allowed` | `export METAFLOW_USER=tester` |
| `ModuleNotFoundError` during collection | `rm -rf .tox && tox -c test/core/tox.ini -e core-local` |
| `core-gcs` — `ConnectionRefusedError` | `cd devtools && SERVICES_OVERRIDE=fake-gcs-server make up` |
| `core-azure` — Azure connection error | `cd devtools && SERVICES_OVERRIDE=azurite make up` |
| `core-batch/k8s` — metadata service error | Start devstack: `cd devtools && make up` |
| Tests run slowly | `tox -c test/core/tox.ini -e core-local -- -n auto` (don't use `-n` for cloud envs) |
