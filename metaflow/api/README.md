# `metaflow.api`
This branch has a few prototype improvements to Metaflow:

- [Features](#features)
- [TODOs](#todo)
- [Examples](#examples)

## Features <a id="features"></a>
1. [More flexible Flow instantiation (relax 1 flow ⟺ 1 file requirement)](#cli)
2. [Directly test flows (without a separate API for writing test-specific flows)](#tests)
3. [Support flow composition (via alternate flow-creation API)](#flows)

### 1. More flexible Flow instantiation (relax 1 flow ⟺ 1 file requirement) <a id="cli"></a>

Basic command-line invocation:
```diff
-python my_flow.py <help|run|show|…> …
+metaflow flow my_flow.py <help|run|show|…> …
```

Per-flow `__main__` handler no longer necessary:
```diff
# my_flow.py
class MyFlow(FlowSpec):
    …
-
-if __name__ == '__main__':
-    MyFlow()
```

Multiple flows can be defined in the same file, and run like:
```bash
metaflow flow my_flow.py:MyFlow1 <help|run|show|…> …
metaflow flow my_flow.py:MyFlow2 <help|run|show|…> …
```
etc.

[`test_cli.py`](../tests/test_cli.py) verifies that a few CLI forms work (including versions where `__main__` exists, which can still optionally be used to specify a flow to be invoked via `python <file> …`)

### 2. Directly test flows (without separate API for "test" flows) <a id="tests"></a>
Metaflow uses a [test harness](https://docs.metaflow.org/internals-of-metaflow/testing-philosophy#specifications) that requires Flows to be written against a different API than users write production flows against. I'm not aware of existing tests of flows written against the production API.

Much of what the test harness provides can be achieved using `pytest` directly (especially [`parametrize`](https://docs.pytest.org/en/6.2.x/parametrize.html), without writing flows against a separate API. This branch demonstrates a few ways to use `pytest` to run end-to-end flow tests:

```python
from metaflow import FlowSpec, step

class OldFlow(FlowSpec):
    @step
    def start(self):
        self.a = 1
        self.next(self.end)
    @step
    def end(self):
        self.b = 2


from metaflow.api import Flow, step

class NewFlow(FlowSpec, metaclass=Flow):
    @step
    def start(self):
        self.a = 1
    @step
    def end(self):
        self.b = 2


# Test old- and new-style flows
from metaflow.tests.utils import parametrize, run

@parametrize('flow', [ OldFlow, NewFlow ])
def test_simple_foreach(flow):
    data = run(flow)
    assert (data.a, data.b) == (1, 2)
```

Check out the tests under [`metaflow/tests`](../tests); some highlights:

#### [`test_simple_foreach.py`](../tests/test_simple_foreach.py)
Definition of a simple `foreach`/`join` flow, in old and new styles, and a parameterized test case that runs each and verifies their data artifact outputs.

#### [`test_api.py`](../tests/test_api.py)
- contains the same flow implemented against the current (`OldFlow`) and new (`NewFlow`) APIs
- for each flow-type:
  - verify important graph properties
  - run the flow, verify data artifacts

#### [`test_cli.py`](../tests/test_cli.py)
- for each of 2 CLI entrypoints, `metaflow …` and `python -m metaflow.main_cli`:
  - verify stdout+stderr from running `<metaflow> flow <file>:<flow> show`
  - verify a run of each {`OldFlow`,`NewFlow`} from `test_api.py`

#### [`tests/utils.py`](../tests/utils.py)
This file contains a few simple utilities for testing real flows directly ([example](../tests/test_api.py)), without (re)writing them against a separate flow-testing API.

### 3. Support flow composition (via alternate flow-creation API) <a id="flows"></a>
There is currently no way to compose flows or steps (cf. [#245](https://github.com/Netflix/metaflow/issues/245)). Several aspects of Metaflow's design impede flow-composition:
- Requiring `self.next` calls in step bodies: this couples each step's logic to assumptions about what is done with its outputs.
- Special handling/presence of `start`+`end` steps complicates composition via inheritance.
- Requirement that each flow lives in its own file (see [**1.**](#files)) and only one flow is imported per Python process.

Many of these issues also make flow-creation more verbose than necessary.

The `metaflow.api` package contains a prototype, alternate API for writing flows. It aims for:

#### Reduced boilerplate
- [x] `self.next` call is synthesized from information found in step-function decorators; graph structure is decoupled from steps' logic.
- [x] If absent, trivial `start`/`end` steps are generated under the hood; only meaningful steps need be provided.
- [x] Graph structure is inferred, where possible:
  - Steps frequently follow the step immediately preceding them; Metaflow just figures that out now.
  - See [`stats_02.py`](../tests/flows/stats_02.py) for an example `@foreach` followed by a `@join` whose `in_funcs` is inferred to be the preceding `@foreach`
- [x] `__name__ == '__main__'` handler optional (see [**1.**](#files))

#### Increased flow composition/reuse
- [x] inheritance (see [`test_flow_inheritance.py`](../tests/test_flow_inheritance.py))
- [ ] composition (reuse Flows)

#### Improved ergonomics
- [x] `foreach`, `join` decorators (see [`stats_02.py`](../tests/flows/stats_02.py), [`test_joins.py`](../tests/test_joins.py))
  - info about next steps is provided by decorators on those next steps (rather than inserted into the end of the body of previous steps; also facilitates reuse)
- [x] Optionally receive `self.input` as an argument to `@foreach` step (rather than having to reference `self.input` at start of step; see [`NewJoinFlow1`](../tests/flows/joins.py))
- [x] Make `metaflow flow <file> …` default to single Flow in file
- [x] Unittest `metaflow flow <file>:<flow> …` invocation style
- [ ] Investigate using a `@flow` class-decorator instead of [`metaclass=Flow`](flow.py)
- [ ] Get Pylint to accept self.input references in Flows w/o FlowSpec explicitly specified

## TODOs <a id="todo"></a>
In addition to the tasks listed above, some general correctness/completeness TODOs:
- [x] Test `split-and`+`join` combo (see [`test_joins.py`](../tests/test_joins.py))
- [x] Integrate new `pytest` tests in CI ([example GHA run](https://github.com/celsiustx/metaflow/runs/2616959407))
- [ ] Implement "conditional" decorators (`@iff`, `@ifn`)
- [ ] Check overloaded Flow basenames / using FQNs
- [ ] Investigate better ways to infer `cls.__file__` on old- and new-style flows
- [ ] Testing: use fresh metaflow db in tempdirs for each case/suite
- [ ] Investigate restoring Python 2 in CI (it was removed to get CI passing; failure was a `SyntaxError` related to default kwargs in some new code in this package)

## Examples <a id="examples"></a>

### Basic Flow <a id="basic-flow"></a>
Here's an example diff, taken from [`test_api.py`](../tests/test_api.py) of an `OldFlow` (written against the existing `FlowSpec` API) and a `NewFlow` (using the new `metaflow.api`):

```diff
-from metaflow import FlowSpec, step
+from metaflow.api import Flow, step

+class NewFlow(metaclass=Flow):
-class OldFlow(FlowSpec):
-    def start(self):
-        self.next(self.one)
     @step
     def one(self):
         self.a = 111
-        self.next(self.two)
     @step
     def two(self):
         self.b = self.a * 2
-        self.next(self.three)
     @step
     def three(self):
         assert (self.a, self.b) == (111, 222)
-        self.next(self.end)
-    @step
-    def end(self): pass
     @property
     def foo(self): return 'some property `foo`'
     def mth(self): return 'some method `mth`'
-
-if __name__ == '__main__':
-    OldFlow()
```

### `@foreach`/`@join` <a id="foreach-join"></a>
Here's a more complex diff:
- before: [`tutorials/02-statistics/stats.py`](../tutorials/02-statistics/stats.py)
- after: [`tests/flows/stats_02.py`](../tests/flows/stats_02.py)

These flows are checked for correctness and concordance in [`test_foreach.py`](../tests/test_foreach.py):

```diff
-from metaflow import FlowSpec, step, IncludeFile
+from metaflow import IncludeFile
+from metaflow.api import Flow, step, foreach, join


-class MovieStatsFlow(FlowSpec):
+class MovieStatsFlow(metaclass=Flow):
     movie_data = IncludeFile("movie_data",
                              help="The path to a movie metadata file.",
                              default=script_path('movies.csv'))

     @step
     def start(self):
         import pandas
         from io import StringIO
         self.dataframe = pandas.read_csv(StringIO(self.movie_data))
         self.genres = {genre for genres \
                        in self.dataframe['genres'] \
                        for genre in genres.split('|')}
         self.genres = list(self.genres)
-        self.next(self.compute_statistics, foreach='genres')
-
-    @step
-    def compute_statistics(self):
-        self.genre = self.input
+    @foreach('genres')
+    def compute_statistics(self, genre):  # optionally receive self.input as an argument
+        self.genre = genre
         print("Computing statistics for %s" % self.genre)
         selector = self.dataframe['genres'].\
                    apply(lambda row: self.genre in row)
         self.dataframe = self.dataframe[selector]
         self.dataframe = self.dataframe[['movie_title', 'genres', 'gross']]
         points = [.25, .5, .75]
         self.quartiles = self.dataframe['gross'].quantile(points).values
-        self.next(self.join)
-
-    @step
+    @join
     def join(self, inputs):
         self.genre_stats = {inp.genre.lower(): \
                             {'quartiles': inp.quartiles,
                              'dataframe': inp.dataframe} \
                             for inp in inputs}
-
-        self.next(self.end)
-
-    @step
-    def end(self):
-        pass
-
-
-if __name__ == '__main__':
-    MovieStatsFlow()
```
