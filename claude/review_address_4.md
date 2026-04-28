# Address — Round 4

## Summary
Apply the lowest-risk tox.ini de-duplications from the round-4 review. Defer
`passenv` tightening (security improvement, but needs CI verification before
landing). Defer F8 (move `_DEFAULT_RUN_OPTIONS` to fixture) to round 5 since
it bundles with the `core_context` dataclass refactor.

## Per-finding verdicts

| ID | Verdict | Reasoning |
|----|---------|-----------|
| F-tox-1 | accept, applied | `[testenv:core-azure]`'s `deps =` block was a verbatim copy of `[testenv]`. Tox inheritance works; delete. |
| F-tox-2 | accept, applied | `[testenv:core-gcs]` now uses `deps = {[testenv]deps}` + extras instead of redeclaring. |
| F-tox-3 | **reject for now** | `passenv = *` is a real security improvement to tighten, but the explicit allow-list needs CI verification across all 7 envs to avoid breaking. Recommend in gist; do not land in local commit. |
| F-tox-4 | reject | `-n 1` on cloud envs is intentional rate-limiting; touching it risks slowing CI. Document in TESTING.md instead. |
| F-tox-5 | accept, applied | Factored `AWS_ACCESS_KEY_ID`/`_SECRET_ACCESS_KEY`/`_ENDPOINT_URL_S3`/`_DEFAULT_REGION` into a shared `[_aws_minio]` section interpolated by 4 cloud envs. |
| F8 (re-raised) | defer to round 5 | Bundle with `core_context` dataclass refactor. |
| F-pytest-1 | reject | Per-test 1800s timeout is the right shape; per-marker overrides not worth the complexity. |

## Changes applied

`test/core/tox.ini`:

1. Deleted `deps = …` block in `[testenv:core-azure]` (4 lines).
2. Replaced `[testenv:core-gcs]` `deps =` block with:
   ```
   deps =
       {[testenv]deps}
       google-cloud-storage
   ```
   (5 lines → 3 lines)
3. Added a new `[_aws_minio]` section with the 4 shared MinIO/AWS env vars; replaced the 4-line block in each of `core-batch`, `core-k8s`, `core-argo`, `core-sfn` with a single `{[_aws_minio]setenv}` interpolation.

## Verification

```
$ /home/coder/.venv/bin/python -m pytest test/core/test_core_pytest.py --collect-only -q | tail -3
========================= 502 tests collected in 0.27s =========================

$ /home/coder/.venv/bin/python -m tox -c test/core/tox.ini config -e core-batch | head -8
[testenv:core-batch]
type = VirtualEnvRunner
set_env =
  AWS_ACCESS_KEY_ID=rootuser
  AWS_DEFAULT_REGION=us-east-1
  AWS_ENDPOINT_URL_S3=http://localhost:9000
  AWS_SECRET_ACCESS_KEY=rootpass123
  ...
```

`{[_aws_minio]setenv}` interpolation resolves correctly in core-batch (and by symmetry in core-k8s, core-argo, core-sfn).

`{[testenv]deps}` interpolation resolves correctly for core-gcs:

```
$ /home/coder/.venv/bin/python -m tox -c test/core/tox.ini config -e core-gcs | grep -A 6 'deps ='
deps =
  -e /home/coder/metaflow/test/core/../../[dev]
  -e /home/coder/metaflow/test/core/../../test/extensions/packages/card_via_extinit
  -e /home/coder/metaflow/test/core/../../test/extensions/packages/card_via_init
  -e /home/coder/metaflow/test/core/../../test/extensions/packages/card_via_ns_subpackage
  google-cloud-storage
```

### Pre-existing bug noted

`tox config -e core-azure` reports `InvalidMarker('Expected a marker variable or quoted string ... AccountName=devstoreaccount1;...')`. The `;` characters in `AZURE_STORAGE_CONNECTION_STRING` confuse tox 4's marker parser. Confirmed pre-existing (reproduces against `HEAD~1`); **not** caused by this round's changes. Surface in the final gist as a follow-up: the connection string should be wrapped in shell-style quoting or moved to `passenv` from a developer-set env var.

## Test-speed impact this round

None for tests themselves (this is config-only). Tox env materialisation is marginally faster because there's less duplicate dep resolution.
