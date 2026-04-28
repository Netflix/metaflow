Review scope:

- The request named `86b1981d`, but while I was reviewing the repo advanced to `702dea91ea1bed7be9c8931029bc6cd0cbc20f81` (`pytest-native: batch 3 — 5 more tests`). The findings below target files that were already present at `86b1981d`, unless a finding explicitly mentions later-added migrations.
- The requested collect-only command on the current worktree reports `60 tests collected`, which reflects the extra batch-3 files now in `HEAD`. The `86b1981d` subset still maps to the expected `36` items.

Finding 1

- Severity: critical
- File: `test/core/tox.ini:116`; `test/core/tox.ini:131`; `test/core/conftest.py:93`
- Snippet (verbatim)
```ini
[testenv:core-argo]
...
commands = pytest {toxinidir} -m argo -n 1 {posargs}

[testenv:core-sfn]
...
commands = pytest {toxinidir} -m sfn -n 1 {posargs}
```

```python
@pytest.fixture(params=["cli", "api"])
def executor(request) -> str:
    """Parametrise tests over both executors.
```
- Suggested fix (concrete code, not vague): Add `METAFLOW_CORE_EXECUTORS` to each tox env (`cli api` for local/azure/gcs/batch/k8s, `scheduler` for argo/sfn) and make `executor` read it instead of hard-coding `["cli", "api"]`. Re-add a `scheduler` branch in `metaflow_runner` that mirrors the old `create`/`trigger`/poll logic from `run_tests.py`. In the same pass, add `METAFLOW_CORE_DISABLED_TESTS` (or explicit per-file pytest marks) so batch/k8s/argo/sfn can keep skipping tests that were disabled in `contexts.json` such as `RunIdFileTest` and `TimeoutDecoratorTest`.
- Rationale (1-3 sentences): Right now `top_options` is the only backend-specific fixture; `executor` never varies by backend. That means `tox -e core-argo` and `tox -e core-sfn` no longer exercise Argo/Step Functions at all, and the old per-backend skip matrix is gone with it.

Finding 2

- Severity: major
- File: `test/core/tox.ini:66`; `test/core/tox.ini:81`; `test/core/tox.ini:93`; `test/core/tox.ini:108`; `test/core/tox.ini:123`; `test/core/tox.ini:137`; `test/core/conftest.py:171`
- Snippet (verbatim)
```ini
METAFLOW_DATASTORE_SYSROOT_AZURE = az://metaflow-test/metaflow/{{nonce}}
METAFLOW_DATASTORE_SYSROOT_GS = gs://metaflow-test/metaflow/{{nonce}}
METAFLOW_DATASTORE_SYSROOT_S3 = s3://metaflow-test/metaflow/{{nonce}}
```

```python
def _build_subprocess_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
```
- Suggested fix (concrete code, not vague): Before every run, build one env dict for both CLI and API paths and expand any value containing `{nonce}`/`{{nonce}}` with a fresh `uuid.uuid4()`. For example, add a helper that walks `os.environ.items()`, rewrites matching values, and then pass that same dict to both `subprocess.run(..., env=env)` and `Runner(..., env=env, ...)`.
- Rationale (1-3 sentences): `origin/master` expanded `{nonce}` per test case before launching flows. The new tox file kept the placeholders, but the new harness never expands them, so cloud-backend runs share a fixed datastore prefix and can collide across tests, executors, or repeated tox invocations.

Finding 3

- Severity: major
- File: `test/core/conftest.py:229`; `test/core/conftest.py:267`
- Snippet (verbatim)
```python
run_id_file = tmp_path / "run-id"
...
run_id = run_id_file.read_text().strip() if run_id_file.exists() else None
```
- Suggested fix (concrete code, not vague): Delete the file before every invocation, e.g. `run_id_file.unlink(missing_ok=True)` immediately before both the CLI `subprocess.run(...)` call and the API `r.run()/r.resume()` call. If you want extra safety, use distinct files such as `run-id.run` and `run-id.resume` for two-phase tests.
- Rationale (1-3 sentences): Resume tests call `metaflow_runner` twice in the same `tmp_path`. If the second invocation fails before rewriting `run-id`, the fixture returns the stale run id from the first attempt and any later `result.run()` or metadata assertions inspect the wrong run.

Finding 4

- Severity: minor
- File: `test/core/metaflow_test/__init__.py:47`
- Snippet (verbatim)
```python
# Backward-compat alias for any pre-migration step bodies that still import
# the old name. Will be removed once nothing references it.
TestRetry = RetryRequested
```
- Suggested fix (concrete code, not vague): Remove the alias once the remaining migrations stop importing it. If the alias must stay temporarily, set `TestRetry.__test__ = False` right after the assignment so `from metaflow_test import TestRetry` cannot reintroduce pytest collection of a fake test class.
- Rationale (1-3 sentences): The whole point of renaming `TestRetry` to `RetryRequested` was to avoid pytest treating it as a test class. Re-exporting `TestRetry` keeps the same footgun alive for the next migrated module that uses the old import.

Finding 5

- Severity: major
- File: `test/core/tests/test_basic_parameters.py:8`; `test/core/tests/test_basic_parameters.py:38`
- Snippet (verbatim)
```python
class BasicParametersFlow(FlowSpec):
    int_param = Parameter("int_param", default=123, type=int)
    bool_param = Parameter("bool_param", default=True, type=bool)
    str_param = Parameter("str_param", default="default_str")
```

```python
monkeypatch.setenv("METAFLOW_RUN_BOOL_PARAM", "False")
monkeypatch.setenv("METAFLOW_RUN_NO_DEFAULT_PARAM", "test_str")
```
- Suggested fix (concrete code, not vague): Port the old parameter matrix back almost verbatim: add `no_default_param`, `bool_true_param`, `list_param`, and `json_param`; assert their parsed values inside the flow; and verify parameter immutability by attempting reassignment in a step and expecting `AttributeError`. Then assert the captured values from both `start` and `end`, not just one step.
- Rationale (1-3 sentences): The original `basic_parameters.py` covered no-default env overrides, boolean truthiness, list parsing, JSON parsing, and immutability. The migrated version now sets `METAFLOW_RUN_NO_DEFAULT_PARAM` for a parameter that no longer exists and only checks the int/bool/string happy path.

Finding 6

- Severity: major
- File: `test/core/tests/test_resume_end_step.py:22`
- Snippet (verbatim)
```python
def test_resume_end_step(metaflow_runner, executor):
    # First run fails inside `end` via ResumeFromHere.
    first = metaflow_runner(ResumeEndStepFlow, executor=executor)
    assert not first.successful
...
    run = resumed.run()
    assert run["start"].task.data.data == "start"
    assert run["end"].task.data.data == "foo"
```
- Suggested fix (concrete code, not vague): Keep the artifact assertions, but also load `first.run()` and compare resume metadata against it. Assert that every cloned task has the same `origin-run-id`, that cloned tasks keep `origin-task-id`, that the rerun `end` task does not, and that non-excluded metadata matches the original task metadata just like the old `check_results` loop did.
- Rationale (1-3 sentences): The original test was mostly about resume metadata correctness, not just “resume flips one artifact from `bar` to `foo`”. The migrated test can pass even if cloned tasks point at the wrong origin run/task or lose metadata during resume.

Finding 7

- Severity: major
- File: `test/core/tests/test_merge_artifacts.py:23`
- Snippet (verbatim)
```python
@step
def join(self, inputs):
    # merge_artifacts pulls non-conflicting artifacts into self.
    self.merge_artifacts(inputs)
    assert self.left_data == "L"
    assert self.right_data == "R"
    assert self.shared == "from-left"
```
- Suggested fix (concrete code, not vague): Split this into a happy-path test and an error-path test. Port back the old cases that (1) conflicting values raise `UnhandledInMergeArtifactsException`, (2) passing both `include` and `exclude` raises `MetaflowException`, (3) calling `merge_artifacts` outside a join raises `MetaflowException`, and then keep the existing successful merge as the final phase.
- Rationale (1-3 sentences): The original `merge_artifacts.py` was mostly validating failure semantics and partial-merge rules. The migrated version only proves the easiest case where both branches already agree, so several real regressions in `merge_artifacts` would now go undetected.

Finding 8

- Severity: major
- File: `test/core/tests/test_lineage.py:1`; `test/core/tests/test_lineage.py:22`
- Snippet (verbatim)
```python
"""metaflow client artifact lineage should match the flow's actual data dependencies."""
```

```python
def test_lineage(metaflow_runner, executor):
    result = metaflow_runner(LineageFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    assert run["start"].task.data.x == 1
    assert run["middle"].task.data.y == 2
    assert run["end"].task.data.z == 3
```
- Suggested fix (concrete code, not vague): Restore the old lineage artifact logic instead of checking arithmetic. A pytest-native port can still compute `self.lineage` in each step and compare it against an expected traversal assembled in the test, or use a small branch/join flow where lineage is not trivially identical to plain artifact propagation.
- Rationale (1-3 sentences): This migrated test no longer exercises lineage at all; it only checks that `1 -> 2 -> 3` arithmetic worked. The docstring still describes the old invariant, but none of the assertions touch it.

Finding 9

- Severity: major
- File: `test/core/tests/test_catch_retry.py:7`; `test/core/tests/test_catch_retry.py:31`
- Snippet (verbatim)
```python
class CatchRetryFlow(FlowSpec):
    @retry(times=3)
    @step
    def start(self):
        self.attempts = current.retry_count
        if current.retry_count < 3:
            raise RetryRequested()
```

```python
def test_catch_retry(metaflow_runner, executor):
    result = metaflow_runner(CatchRetryFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    # @retry: started 4 times (0, 1, 2, 3); the 4th succeeded.
    assert run["start"].task.data.attempts == 3
    # @catch: middle records the exception via the configured var.
    caught = run["middle"].task.data.caught
    assert "explicitly fail middle" in str(caught.exception)
```
- Suggested fix (concrete code, not vague): Port the old retry/catch cases into a couple of focused pytest-native flows/tests: one for retry metadata/logs on `start`, one for retry on split/join steps, and one for `@catch` interaction. Assert the `attempt` metadata sequence on each task, verify that failed-attempt-only artifacts like `invisible` are absent from the final task data, and check that `task.exception` is `None` once `@catch` handled the exception.
- Rationale (1-3 sentences): The original `catch_retry.py` covered retry metadata, log preservation, split/join retry behavior, and `@catch` semantics on both success and failure. The migrated test now proves only one linear retry count and one caught exception string.

TOP 5 FIXES

1. Restore backend correctness in the harness: reintroduce backend-specific executors (`scheduler` for Argo/SFN) and the old disabled-tests matrix.
2. Reintroduce per-test `{nonce}` expansion and pass the exact same expanded env to both CLI and API execution paths.
3. Delete or rotate `run-id` files between `run()` and `resume()` so resumed/failing tests cannot read stale run ids.
4. Restore the missing negative-path coverage in `test_resume_end_step.py`, `test_merge_artifacts.py`, and `test_catch_retry.py`.
5. Restore the original semantic coverage in `test_basic_parameters.py` and `test_lineage.py` before migrating more files on top of the current pattern.
