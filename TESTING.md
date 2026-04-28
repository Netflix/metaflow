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

## What to run locally vs in CI

```
unit tests  →  core-local  →  open PR  →  CI handles the rest
```

| Suite | Command | Needs | Run where |
|-------|---------|-------|-----------|
| Unit | `tox` | nothing | locally + CI |
| Core — local | `tox -c test/core/tox.ini -e core-local` | nothing | locally + CI |
| Core — GCS / Azure | `tox -c test/core/tox.ini -e core-{gcs,azure}` | emulator at known port | **CI** |
| Core — Batch/K8s/Argo/SFN | `tox -c test/core/tox.ini -e core-<name>` | full devstack | **CI** |
| UX — local | `tox -e ux-local` | nothing | locally + CI |
| UX — cloud | `tox -e ux-<name>` | full devstack | **CI** |

Run **unit + core-local** before every PR. All cloud-backend tests require
infrastructure that CI provisions — there is no need to set up emulators or
Docker containers locally. Open your PR and let CI handle them.

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

### core-local — run this locally

```bash
tox -c test/core/tox.ini -e core-local

# Filters (useful when iterating on a fix)
tox -c test/core/tox.ini -e core-local -- --core-tests BasicArtifact
tox -c test/core/tox.ini -e core-local -- --core-graphs simple-foreach
tox -c test/core/tox.ini -e core-local -- -n auto   # parallel
```

### core-gcs / core-azure / core-batch / core-k8s / core-argo / core-sfn — let CI run these

These backends require external infrastructure (GCS emulator, Azure emulator,
MinIO, Kubernetes, …). CI provisions all of it automatically. You do not need
to install Docker or start any services to get these tests to pass.

If you are debugging a specific backend failure locally and already have the
required infrastructure running, you can invoke the env directly:

```bash
tox -c test/core/tox.ini -e core-gcs    # expects fake-gcs-server at localhost:4443
tox -c test/core/tox.ini -e core-azure  # expects azurite at localhost:10000
tox -c test/core/tox.ini -e core-batch  # expects devstack (see devtools/README.md)
```

For devstack setup see `devtools/README.md`.

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
| `core-local` slow | `tox -c test/core/tox.ini -e core-local -- -n auto` |
| Cloud env fails locally | Check that the required emulator/devstack is running — or just open a PR and let CI handle it |
