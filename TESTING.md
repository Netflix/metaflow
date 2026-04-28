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

## Bootstrap testing with mli-metaflow-custom

Netflix runs Metaflow in production via an internal extension layer
([`corp/mli-metaflow-custom`](https://github.netflix.net/corp/mli-metaflow-custom)).
The *bootstrap test* verifies that a given OSS commit installs correctly under that
layer and that the combined test suite passes.

### How to trigger

Apply the **`testable`** label to your OSS PR on GitHub
(`https://github.com/Netflix/metaflow/pulls`).

A maintainer must apply the label — external contributors should request it in
the PR description or a comment.

**What happens next (automatically):**

1. A Netflix webhook detects the label event and calls the internal trigger service.
2. The trigger service opens a PR in `mli-metaflow-custom` that pins `OSS_VERSION`
   to the exact commit SHA of your OSS branch at the moment the label was applied.
3. Jenkins runs the *bootstrap testing flow* on that internal PR:
   - Clones OSS metaflow at the pinned commit.
   - Merges the OSS test files (`test/core/tests/`, `test/unit/`, `test/data/`, etc.)
     into the internal suite.
   - Installs mli-metaflow-custom on top of the OSS package.
   - Runs the full combined test suite on Titus.
4. The bootstrap flow posts a pass/fail comment back to your OSS PR and, on success,
   adds the **`mergeable`** label.

### Important: label security and re-triggering

The `testable` (and `mergeable`) labels are **removed automatically** when the test is
triggered.  This is intentional — it prevents a PR from being tested against a
different commit than the one a maintainer reviewed.

If you push new commits after the label is applied, you must ask a maintainer to
**re-apply `testable`** to trigger a fresh run against the latest commit.

### S3 / MinIO tests (`ok-to-test`)

A separate GitHub Actions workflow
(`.github/workflows/metaflow.s3_tests.minio.yml`) runs the S3 data-layer tests
against a local MinIO instance.  This workflow is also label-gated:

| Label | Triggers |
|-------|----------|
| `testable` | Bootstrap tests in mli-metaflow-custom |
| `ok-to-test` (or `approved`) | S3/MinIO GitHub Actions workflow |

Both labels are normally applied together when a PR is ready for full CI coverage.

### Reading the results

- Bootstrap pass/fail: look for a comment from the bootstrap flow bot on your OSS PR.
- S3 tests: check the **Actions** tab or the CI status checks at the bottom of the PR.
- When the bootstrap passes: the `mergeable` label appears on the OSS PR.

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
