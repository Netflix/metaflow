# Round 4 Review — `test/core/tox.ini`, `test/core/pytest.ini`, and the `_DEFAULT_RUN_OPTIONS` global

## Context
- HEAD: `116f438b review round 3`. Framework dead-surface cleared.
- Baseline: 502 tests collected.

## Findings

### F-tox-1 — `[testenv:core-azure]` redeclares `deps =` with identical content as `[testenv]`
**Severity:** minor
**File:** `test/core/tox.ini:56-61`

```ini
[testenv:core-azure]
deps =
    -e {toxinidir}/../../[dev]
    -e {toxinidir}/../../test/extensions/packages/card_via_extinit
    -e {toxinidir}/../../test/extensions/packages/card_via_init
    -e {toxinidir}/../../test/extensions/packages/card_via_ns_subpackage
```

The four lines exactly match `[testenv]`'s `deps =`. Tox inherits `[testenv]` deps when a child env doesn't declare its own; the redeclaration **replaces**, but here it replaces with the same set. Pure duplication.

**Suggested fix:** Delete the entire `deps =` block in `[testenv:core-azure]`. Inheritance does the right thing.

**Rationale:** Less to update when a base dep changes; less to read.

---

### F-tox-2 — `[testenv:core-gcs]` redeclares 4 deps just to add 1
**Severity:** minor
**File:** `test/core/tox.ini:75-81`

```ini
[testenv:core-gcs]
deps =
    -e {toxinidir}/../../[dev]
    -e {toxinidir}/../../test/extensions/packages/card_via_extinit
    -e {toxinidir}/../../test/extensions/packages/card_via_init
    -e {toxinidir}/../../test/extensions/packages/card_via_ns_subpackage
    google-cloud-storage
```

`google-cloud-storage` is the only addition; the other four lines are duplicated from `[testenv]`.

**Suggested fix:**

```ini
[testenv:core-gcs]
deps =
    {[testenv]deps}
    google-cloud-storage
```

**Rationale:** Single source of truth for base deps. Tox supports section interpolation natively.

---

### F-tox-3 — `[testenv]` uses `passenv = *` (leaks every parent env var into tests)
**Severity:** major
**File:** `test/core/tox.ini:10`

```ini
[testenv]
passenv = *
```

`passenv = *` defeats tox's whole-env-isolation contract: every variable set in the developer's shell (AWS keys, kubeconfig, terraform vars, OAuth tokens) is forwarded into the test process. With the harness now also writing those values into the test subprocess via `subprocess.run(env=env)`, that data ends up in flow processes too.

The variables actually needed are: `PATH`, `HOME`, `USER`, `LANG`, `LC_ALL`, `TERM`, plus a small allow-list of `AWS_*` / `AZURE_*` / `GOOGLE_*` for the cloud envs that need them.

**Suggested fix:**

```ini
[testenv]
passenv =
    PATH
    HOME
    USER
    LANG
    LC_ALL
    TERM
    PYTHONPATH

[testenv:core-batch]
passenv =
    {[testenv]passenv}
    AWS_*
```

**Rationale:** Reduces the blast radius of a misconfigured developer shell, makes the env contract explicit. `passenv = *` is documented as a last resort in tox docs.

---

### F-tox-4 — Scheduler envs hardcode `-n 1` instead of using a marker
**Severity:** minor
**File:** `test/core/tox.ini:156, 182`

```ini
[testenv:core-argo]
...
commands = pytest {toxinidir} -m argo -n 1 {posargs}

[testenv:core-sfn]
...
commands = pytest {toxinidir} -m sfn -n 1 {posargs}
```

`-n 1` disables xdist parallelism. Same goes for cloud envs (gcs, batch, k8s, etc.). Reason: each backend has limited concurrent capacity (one Argo controller, one SFN account). But hardcoding it here means a developer running `tox -e core-argo -- -n auto` to debug parallelism gets it overridden.

**Suggested fix:** Mark the FlowDefinition class or the parametrization with `pytest.mark.serial` and use a `pytest.ini` `addopts = -p no:xdist` *for those markers only*, or document it instead. For now, keep — but note in `TESTING.md`.

**Verdict for this round:** **defer**. Not a clear win; touching this risks slowing CI. Document as round-5 follow-up.

---

### F-tox-5 — Cloud envs duplicate ~6 lines of MinIO/AWS config
**Severity:** nit
**File:** `test/core/tox.ini:94-181` (core-batch / core-k8s / core-argo / core-sfn)

```ini
AWS_ACCESS_KEY_ID = rootuser
AWS_SECRET_ACCESS_KEY = rootpass123
AWS_ENDPOINT_URL_S3 = http://localhost:9000
AWS_DEFAULT_REGION = us-east-1
```

These four lines repeat in all four AWS-using envs. Tox supports per-section `setenv` interpolation:

```ini
[_aws_minio]
setenv =
    AWS_ACCESS_KEY_ID = rootuser
    AWS_SECRET_ACCESS_KEY = rootpass123
    AWS_ENDPOINT_URL_S3 = http://localhost:9000
    AWS_DEFAULT_REGION = us-east-1

[testenv:core-batch]
setenv =
    {[testenv]setenv}
    {[_aws_minio]setenv}
    ...
```

**Suggested fix:** Apply the pattern. Saves ~16 lines net.

**Verdict for this round:** apply.

---

### F8 (re-raised from round 1) — Move `_DEFAULT_RUN_OPTIONS` and `_SASHIMI` into a `core_context` session fixture
**Severity:** minor
**File:** `test/core/test_core_pytest.py:45-52`

Currently:

```python
_SASHIMI = "刺身 means sashimi"

_DEFAULT_RUN_OPTIONS = [
    "--max-workers=50",
    "--max-num-splits=10000",
    "--tag=%s" % _SASHIMI,
    "--tag=multiple tags should be ok",
]
```

These are the run-options the harness passes to the cli/api executor. Currently a global; better as a fixture so a `conftest.py` closer to a test directory could override.

**Verdict for this round:** **defer to round 5** — small, low-impact; no callers needing override yet. Bundle with the core_context dataclass refactor (F6).

---

### F-pytest-1 — `pytest.ini`'s timeout of 1800s is per-item, but most local tests finish in <5s
**Severity:** nit
**File:** `test/core/pytest.ini:8`

```ini
timeout = 1800
```

Per-test 30-minute timeout makes sense for cloud / scheduler tests that wait on external infrastructure. Local tests would benefit from a tighter cap to fail fast on hangs.

**Verdict:** keep — the harness uses pytest-timeout and overriding per-mark is not a clean win. **No action.**

## TOP simplifications (round 4)

1. **Delete redundant `deps =` block in `[testenv:core-azure]`** (F-tox-1)
2. **Switch `[testenv:core-gcs]` to interpolated `deps = {[testenv]deps}` + extras** (F-tox-2)
3. **Tighten `passenv = *`** to an explicit allow-list (F-tox-3)
4. **Factor MinIO/AWS env block into a shared `[_aws_minio]` section** (F-tox-5)

Net result: fewer lines, one source of truth per dep / per env block, and `passenv` becomes explicit so a misbehaving developer shell can't leak credentials into tests.
