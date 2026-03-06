# UX Tests

End-to-end tests for OSS Metaflow features using the Runner API.

## Running the tests

Create a clean virtual environment with the OSS metaflow installed:

```bash
python3 -m venv /tmp/metaflow-test-venv
/tmp/metaflow-test-venv/bin/pip install -e /path/to/metaflow pytest
```

Then run from the repo root:

```bash
/tmp/metaflow-test-venv/bin/pytest test/ux/core/test_basic.py -v
/tmp/metaflow-test-venv/bin/pytest test/ux/core/test_config.py -v
```

### Why a clean venv?

The tests must be run with OSS Metaflow only (no Netflix extensions).
Netflix extensions change the default metadata provider and packaging,
causing tests to hang or fail.

## Test structure

```
test/ux/
├── conftest.py            # Fixtures and parametrization
├── pytest.ini             # Marker definitions
└── core/
    ├── test_basic.py      # Basic flow tests (hello world, project, conda)
    ├── test_config.py     # Config/FlowMutator tests
    ├── test_utils.py      # Shared utilities
    └── flows/
        ├── basic/         # Flow files for basic tests
        └── config/        # Flow files for config tests
```

## Marks

- `basic` — Hello world, project decorator, basic flow patterns
- `config` — `Config`, `FlowMutator`, `StepMutator`, `config_expr`
- `conda` — Requires a conda environment manager
- `scheduler_only` — Requires a remote scheduler (deployer mode only)

## Deployer mode

Tests default to `runner` (local) mode. Pass `--scheduler-type` to also
run in deployer mode against a remote scheduler:

```bash
pytest test/ux/ --scheduler-type argo-workflows --cluster <cluster-name>
```
