# AlgoSpec -- Single-Computation Metaflow Construct

AlgoSpec is a FlowSpec subclass for algorithms that have a single `init()` + `call()` lifecycle -- no multi-step DAG, no `@step`, no `self.next()`. All Metaflow infrastructure (Parameters, decorators, CLI, Maestro) works unchanged.

## Test 1: Local callable

Instantiate the AlgoSpec directly, set parameters, call `init()` once, then call the model on rows:

```bash
python test_algo_spec_local.py
```

Expected output:

```
SquareModel.init() -- multiplier=2.0
{'value': 5, 'result': 50.0}
{'value': 3, 'result': 18.0}
{'value': 10, 'result': 200.0}
```

## Test 2: Maestro deployment

Validate the graph and lint, then deploy to Maestro:

```bash
# Show the graph (single "call" node)
python test_algo_spec.py show

# Validate (lint + pylint)
python test_algo_spec.py check

# Generate Maestro workflow JSON without deploying
python test_algo_spec.py maestro --cluster sandbox create --name square-model-test --only-json

# Deploy to Maestro sandbox
python test_algo_spec.py maestro --cluster sandbox create --name square-model-test
```
