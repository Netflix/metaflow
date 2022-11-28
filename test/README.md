# Metaflow Test Suite

Metaflow test suite consists of two parts:

 1. A data test suite for the data layer components (`S3`) based on
    [Pytest](http://pytest.org). These tests can be found
    under the `test/data` directory.
 2. An integration test harness for the core Metaflow at `test/core`.
    The harness generates and executes synthetic Metaflow flows,
    exercising all aspects of Metaflow.

You can run the tests by hand using `pytest` or `run_tests.py` as described
below.

## Data Test Suite

The data tests are standard `pytest` suites. In the `s3` folder, you will
find two files: `s3_data.py` which generates synthetic
data and `test_s3.py` which contains the actual tests.

The test data is cached in S3. If you change anything in the `s3_data.py`
module (or you have another reason for wanting to regenerate test
data), you can regenerate the data easily by changing the S3 test prefix
at `test/data/__init__.py`. The `s3_data.py` detects that data
is missing in S3 and they will upload the data in the new location
automatically.

### Running data tests by hand

You can run the data tests using `pytest` as follows:

```
cd test/data/
PYTHONPATH=`pwd`/../../ python3 -m pytest -x -s -v --benchmark-skip
```

You can obviously also not skip the benchmarks but be aware that the
benchmarks run for a long time.

Both Python2 and Python3 are supported. See `python -m pytest --help`
for more information about how to execute `pytest` tests.

## The Integration Test Harness for Metaflow

The integration test harness for the core Metaflow at `test/core`
generates and executes synthetic Metaflow flows, exercising all 
aspects of Metaflow. The test suite is executed using 
[tox](http://tox.readthedocs.io) as configured in `tox.ini`. 
You can run the tests by hand using `pytest` or 
`run_tests.py` as described below.

What happens when you execute `python helloworld.py run`? The execution
involves multiple layers of the Metaflow stack. The stack looks like 
following, starting from the most fundamental layer all the way to the 
user interface:

 0. Python interpreter (`python2`, `python3`)
 1. Metaflow core (`task.py`, `runtime.py`, `datastore`, etc.)
 2. Metaflow plugins (`@timeout`, `@catch`, `metadata.py` etc.)
 3. User-defined graph
 4. User-defined step functions
 5. User interface (`cli.py`, `metaflow.client`)

We could write unit tests for functions in the layers 1, 2, and 5,
which would capture some bugs. However, a much larger superset of bugs
is caused by unintended interactions across the layers. For instance,
exceptions caught by the `@catch` tag (2) inside a deeply nested foreach
graph (3) might not be returned correctly in the client API (5) when
using Python 3 (0).

The integration test harness included in the `core` directory tries to
surface bugs like this by generating test cases automatically using
*specifications* provided by the developer.

### Specifications

The test harness allows you to customize behavior in four ways that
correspond to the layers above:

 1. You define the execution environment, including environment
    variables, the version of the Python interpreter, and the type
    of datastore used as *contexts* in `contexts.json` (layers 0 and 1).

 2. You define the step functions, the decorators used, and the
    expected results as `MetaflowTest` templates, stored in the `tests`
    directory (layers 2 and 4).

 3. You define various graphs that match the step functions as
    simple JSON descriptions of the graph structure, stored in the
    `graphs` directory (layer 3).

 4. You define various ways to check the results that correspond to
    the different user interfaces of Metaflow as `MetaflowCheck` classes,
    stored in the `metaflow_test` directory (layer 5). You can customize
    which checkers get used in which contexts in `context.json`.

The test harness takes all `contexts`, `graphs`, `tests`, and `checkers`
and generates a test flow for every combination of them, unless you
explicitly set constraints on what combinations are allowed. The test
flows are then executed, optionally in parallel, and results are
collected and summarized.

#### Contexts

Contexts are defined in `contexts.json`. The file should be pretty
self-explanatory. Most likely you do not need to edit the file unless
you are adding tests for a new command-line argument.

Note that some contexts have `disabled: true`. These contexts are not
executed by default when tests are run by a CI system. You can enable
them on the command line for local testing, as shown below.

#### Tests

Take a look at `tests/basic_artifact.py`. This test verifies that
artifacts defined in the first step are available in all steps
downstream. You can use this simple test as a template for new
tests.

Your test class should derive from `MetaflowTest`. The class variable
`PRIORITY` denotes how fundamental the exercised functionality is to
Metaflow. The tests are executed in the ascending order of priority,
to make sure that foundations are solid before proceeding to more
sophisticated cases.

The step functions are decorated with the `@steps` decorator. Note that
in contrast to normal Metaflow flows, these functions can be applied
to multiple steps in a graph. A core idea behind this test harness is
to decouple graphs from step functions, so various combinations can be
tested automatically. Hence, you need to provide step functions that
can be applied to various step types.

The `@steps` decorator takes two arguments. The first argument is an
integer that defines the order of precedence between multiple `steps`
functions, in case multiple step function templates match. A typical
pattern is to provide a specific function for a specific step type,
such as joins and give it a precedence of `0`. Then another catch-all
can be defined with `@steps(2, ['all'])`. As the result, the special
function is applied to joins and the catch-all function for all other
steps.

The second argument gives a list of *qualifiers* specifying which
types of steps this function can be applied to. There is a set of
built-in qualifiers: `all`, `start`, `end`, `join`, `linear` which
match to the corresponding step types. In addition to these built-in
qualifiers, graphs can specify any custom qualifiers.

By specifying `required=True` as a keyword argument to `@steps`,
you can require that a certain step function needs to be used in
combination with a graph to produce a valid test case. By creating a
custom qualifier and setting `required=True` you can control how tests
get matched to graphs.

In general, it is beneficial to write test cases that do not specify
overly restrictive qualifiers and `required=True`. This way you cast a
wide net to catch bugs with many generated test cases. However, if the
test is slow to execute and/or does not benefit from a large number of
matching graphs, it is a good idea to make it more specific.

##### Assertions

The test case is not very useful unless it verifies its results. There
are two ways to assert that the test behaves as expected.

You can use a function `assert_equals(expected, got)` inside step
functions to confirm that data inside the step functions is valid.
Secondly, you can define a method `check_results(self, flow, checker)`
in your test class, which verifies the stored results after the flow
has been executed successfully.

Use
```
checker.assert_artifact(step_name, artifact_name, expected_value)
```
to assert that steps contain the expected data artifacts.

Take a look at existing test cases in the `tests` directory to get an
idea how this works in practice.

#### Graphs

Graphs are simple JSON representations of directed graphs. They list
every step in a graph and transitions between them. Every step can have
an optional list of custom qualifiers, as described above.

You can take a look at the existing graphs in the `graphs` directory
to get an idea of the syntax.

#### Checkers

Currently, the test harness exercises two types of user interfaces:
The command line interface, defined in `cli_check.py`, and the Python
API, defined in `mli_check.py`.

Currently, you can use these checkers to assert values of data artifacts
or log output. If you want to add test for new type of functionality
in the CLI and/or the Python API, you should add a new method in
the `MetaflowCheck` base class and corresponding implementations in
`mli_check.py` and `cli_check.py`. If certain functionality is only
available in one of the interfaces, you can provide a stub implementation
returning `True` in the other checker class.

### Usage

The test harness is executed by running `run_tests.py`. By default, it
executes all valid combinations of contexts, tests, graphs, and checkers.
This mode is suitable for automated tests run by a CI system.

When testing locally, it is recommended to run the test suite as follows:

```
cd metaflow/test/core
PYTHONPATH=`pwd`/../../ python run_tests.py --debug --contexts dev-local
```

This uses only the `dev_local` context, which does not depend
on any over-the-network communication like `--metadata=service` or
`--datastore=s3`. The `--debug` flag makes the harness fail fast when
the first test case fails. The default mode is to run all test cases and
summarize all failures in the end.

You can run a single test case as follows:

```
cd metaflow/test/core
PYTHONPATH=`pwd`/../../ python run_tests.py --debug --contexts dev-local --graphs single-linear-step --tests BasicArtifactTest
```

This chooses a single context, a single graph, and a single test. If you are developing a new test, this is the fastest way to test the test.



