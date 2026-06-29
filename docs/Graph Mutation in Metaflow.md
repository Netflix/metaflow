

| ![][image1] | MINO Machine Learning Platform |
| :---- | ----: |

# Graph Mutation in Metaflow

---

**Status:**  ✏️  Draft

**Target Audience:** Metaflow developers

**Memo Goal:** Determine best path forward

**Authors:** [Romain Cledat](mailto:rcledat@netflix.com)

**Contributors:** Names

**Last Updated On:** Date

---

#

# Research: Programmatic Graph Mutation for Metaflow

## **Table of Contents**

1. [Executive Summary](#executive-summary)
2. [Current State of Metaflow](#current-state-of-metaflow)
   - [How the DAG is Built](#how-the-dag-is-built)
   - [The Mutation System Today](#the-mutation-system-today)
   - [Key Architectural Constraints](#key-architectural-constraints)
3. [Problem Statement](#problem-statement)
4. [Prior Art: How Other Frameworks Handle This](#prior-art-how-other-frameworks-handle-this)
   - [Apache Airflow](#apache-airflow)
   - [Prefect](#prefect)
   - [Dagster](#dagster)
   - [Hamilton (DAGWorks)](#hamilton-dagworks)
   - [Summary of Approaches](#summary-of-approaches)
5. [Core Design Principle: Decoupling Functions from Topology](#core-design-principle-decoupling-functions-from-topology)
6. [Design Proposals](#design-proposals)
   - [Proposal A: Edge-Centric API](#proposal-a-edge-centric-api)
   - [Proposal B: Positional Step Insertion API](#proposal-b-positional-step-insertion-api)
   - [Proposal C: Declarative Graph Builder](#proposal-c-declarative-graph-builder)
   - [Proposal D: Hybrid (Recommended)](#proposal-d-hybrid-recommended)
7. [End-to-End Scenarios](#end-to-end-scenarios)
8. [Resolved Design Decisions](#resolved-design-decisions)
9. [Open Questions](#open-questions)

---

## **Executive Summary**

Metaflow's `FlowMutator`/`StepMutator` system currently allows programmatic modification of **decorators and parameters** on existing flows, but cannot modify the **graph topology** itself (adding/removing steps or changing edges). This document explores how to extend the `MutableFlow` and `MutableStep` APIs to support full graph construction and modification, enabling use cases from config-driven flow generation to surgical step insertion.

The central insight driving the design is a **separation of concerns**: a step's *function body* (what it computes) should be decoupled from the *graph topology* (where it sits in the DAG). Today, `self.next()` inside the function body is the sole mechanism for defining edges. We propose making `self.next()` optional \-- the graph can be defined externally via the mutation API, and step functions become plain callables that are independently reusable. When `self.next()` is present in a function body, it serves as a default that can be overridden by the mutation layer.

---

## **Current State of Metaflow**

### How the DAG is Built

Metaflow constructs its DAG through a multi-layer process:

**Layer 1: Static Analysis (AST parsing) \-- `graph.py`**

`FlowGraph._create_nodes()` scans the FlowSpec class using `dir()` to find all callables with the `is_step` attribute. For each step, it:

- Retrieves source code via `inspect.getsourcelines()`
- Parses it with `ast.parse()`
- Creates a `DAGNode` that determines node type by analyzing the `self.next()` call at the tail of the function body

This means the graph structure is determined entirely from **source code analysis**, not runtime execution. The `self.next()` call is parsed as AST, not executed.

**Layer 2: Graph Traversal \-- `FlowGraph._traverse_graph()`**

Starting from the `start` node, a depth-first traversal:

- Builds topological ordering (`sorted_nodes`)
- Populates `in_funcs` (predecessors) and validates `out_funcs` (successors)
- Tracks split/join nesting via `split_parents` and `matching_join`
- Identifies foreach boundaries with `is_inside_foreach`

**Layer 3: Runtime Execution \-- `FlowSpec.next()`**

At runtime, the `next()` method validates and executes transitions, handling foreach iteration, condition evaluation for switches, and split fan-out.

**Node Types:**

| Type | `self.next()` Syntax | Semantics |
| :---- | :---- | :---- |
| `start` | `self.next(self.B)` | Entry point, always single successor |
| `linear` | `self.next(self.B)` | Single input, single output |
| `join` | (has `inputs` parameter) | Collects multiple branches |
| `split` | `self.next(self.B, self.C, ...)` | Unconditional fan-out |
| `foreach` | `self.next(self.B, foreach='var')` | Data-driven fan-out |
| `split-switch` | `self.next({k: self.B, ...}, condition='var')` | Conditional routing |
| `end` | (no `self.next()`) | Terminal node |

**Critical Detail: The `@step` marker**

The `@step` decorator (`decorators.py:949-988`) is minimal but foundational:

```py
def step(f):
    f.is_step = True
    f.decorators = []
    f.config_decorators = []
    f.wrappers = []
    f.name = f.__name__
    return f
```

Any callable with `is_step=True` on the FlowSpec class will be discovered by `FlowGraph`.

### The Mutation System Today

**FlowMutator / StepMutator** provide two-phase callbacks:

1. `pre_mutate(mutable_flow/step)` \-- Early phase after config values are resolved. Can add/remove parameters and decorators.
2. `mutate(mutable_flow/step)` \-- Late phase after CLI parsing. Can add/remove step decorators only.

**MutableFlow** currently supports:

- `add_parameter()` / `remove_parameter()` \-- pre\_mutate only
- `add_decorator()` / `remove_decorator()` \-- pre\_mutate only
- `steps` \-- iterate `(name, MutableStep)` tuples
- `configs` / `parameters` / `decorator_specs` \-- read-only iteration
- Direct attribute access: `mutable_flow.start` returns a `MutableStep`

**MutableStep** currently supports:

- `add_decorator()` / `remove_decorator()` \-- step decorators at any phase, StepMutators at pre\_mutate only
- `flow` \-- access parent MutableFlow
- `decorator_specs` \-- read-only iteration

**What is NOT supported today:**

- Adding or removing steps
- Changing edges (`self.next()` targets)
- Creating a flow from scratch (empty FlowSpec)

### Key Architectural Constraints

1. **AST-based edge parsing**: The graph reads `self.next()` from the function's source code via AST parsing. Dynamically created functions (lambdas, closures) may not have `inspect`\-able source code. This is the biggest technical hurdle.

2. **`_init_graph()` is re-callable**: After mutations, `_init_graph()` is called again (`flowspec.py:466`) to rebuild the graph. This means if we can get new step functions onto the class with proper metadata, the graph will pick them up.

3. **Step discovery uses `dir()`**: `FlowGraph._create_nodes()` uses `dir(flow)` \+ `hasattr(func, "is_step")`. Adding a method with `setattr(cls, name, func)` where `func` has `is_step=True` will cause it to be discovered.

4. **Source code requirement**: `inspect.getsourcelines()` is called on every step. Dynamically generated functions need to have inspectable source, or the graph construction must be modified to handle steps without parseable source.

---

## **Problem Statement**

We want to enable two categories of use cases:

### Category 1: Full Flow Construction from Configuration

```py
@make_flow(config="pipeline.yaml")
class MyFlow(FlowSpec):
    pass
```

The entire graph \-- steps, edges, function bodies \-- is generated from external configuration. The class starts essentially empty and the mutator builds everything.

### Category 2: Surgical Graph Modification

Given an existing flow with steps and edges, a mutator should be able to:

- Insert a new step between two existing steps
- Add a new branch to a split
- Remove a step and reconnect edges
- Replace a step's transitions

### Why This Matters

Config-driven pipelines are common in ML platforms. Teams want to define pipeline templates where the structure (which preprocessing steps run, which model trains, what validation occurs) is driven by configuration rather than code. Today, the only way to achieve this in Metaflow is to write a monolithic flow with conditional branches, which quickly becomes unwieldy.

---

## **Prior Art: How Other Frameworks Handle This**

### Apache Airflow

Airflow's DAGs are inherently programmatic Python objects. There is no declarative step definition \-- everything is imperative.

**Task dependency API:**

```py
with DAG("my_dag", schedule=None, start_date=datetime(2021, 1, 1)) as dag:
    task_a = PythonOperator(task_id="task_a", python_callable=fn_a)
    task_b = PythonOperator(task_id="task_b", python_callable=fn_b)
    task_c = PythonOperator(task_id="task_c", python_callable=fn_c)

    task_a >> [task_b, task_c]  # a -> b, a -> c
    task_b >> task_c            # b -> c (so c joins b and the direct edge from a)
```

**Dynamic generation from config:**

```py
for config_name, config in configs.items():
    dag_id = f"dynamic_generated_dag_{config_name}"

    @dag(dag_id=dag_id, start_date=datetime(2022, 2, 1))
    def dynamic_generated_dag():
        @task
        def process(message):
            print(message)
        process(config["message"])

    dynamic_generated_dag()
```

**Key takeaway:** Airflow is fully imperative. DAGs are Python objects, and edges are set with `>>` operators or `set_upstream()`/`set_downstream()`. Very flexible but verbose \-- there's no "flow class" abstraction. The graph is the code.

*Sources: [Airflow Dynamic DAG Generation](https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html), [Astronomer Dependencies Guide](https://www.astronomer.io/docs/learn/managing-dependencies)*

### Prefect

Prefect moved away from static DAGs entirely. In Prefect 2.0+, workflows use native Python control flow \-- no explicit graph definition at all.

```py
@flow
def my_flow(data):
    result_a = task_a(data)
    if result_a > threshold:
        result_b = task_b(result_a)
    else:
        result_b = task_c(result_a)
    return task_d(result_b)
```

**Task mapping (dynamic parallelism):**

```py
@flow
def etl_flow(urls):
    # .map() creates one task per element, returns list of futures
    results = fetch_data.map(urls)
    processed = transform.map(results)
    load(processed)
```

**Key takeaway:** Prefect's philosophy is "just write Python." The graph is implicit in the function call chain. This is maximally flexible but means there's no inspectable graph structure before runtime \-- you can't visualize or validate the DAG ahead of execution. Metaflow's static analyzability is a deliberate design choice that we want to preserve.

*Sources: [Prefect Second-Generation Workflow Engine](https://www.prefect.io/blog/second-generation-workflow-engine), [Prefect Workflow Design Patterns](https://www.prefect.io/blog/workflow-design-patterns)*

### Dagster

Dagster offers both declarative and programmatic graph construction:

**Declarative (`@graph` decorator):**

```py
@dg.graph
def my_pipeline():
    result = step_a()
    branched = step_b(result)
    step_c(branched)
```

**Programmatic (`GraphDefinition`):**

```py
from dagster import GraphDefinition, DependencyDefinition

pipeline = GraphDefinition(
    name="my_pipeline",
    node_defs=[step_a, step_b, step_c],
    dependencies={
        "step_b": {"input": DependencyDefinition("step_a")},
        "step_c": {"input": DependencyDefinition("step_b")},
    },
).to_job()
```

**Dynamic fan-out:**

```py
@op(out=DynamicOut())
def generate_subtasks(context):
    for i in range(N):
        yield DynamicOutput(i, mapping_key=str(i))

@graph
def my_graph():
    subtasks = generate_subtasks()
    results = subtasks.map(process_subtask)
    aggregate(results.collect())
```

**Key takeaway:** Dagster's `GraphDefinition` is the closest prior art to what we need. It separates node definitions from edge wiring, allowing fully programmatic construction. The `dependencies` dict is explicit but verbose. The `@graph` decorator is more ergonomic but requires a function body with Python call syntax.

*Sources: [Dagster Op Graphs](https://docs.dagster.io/guides/build/ops/graphs), [Dagster Dynamic Graphs](https://docs.dagster.io/guides/build/ops/dynamic-graphs)*

### Hamilton (DAGWorks)

Hamilton takes a radically different approach: **function names and parameter names define the DAG**.

```py
# transforms.py
def raw_data(source_path: str) -> pd.DataFrame:
    return pd.read_csv(source_path)

def cleaned_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data.dropna()

def feature_matrix(cleaned_data: pd.DataFrame) -> pd.DataFrame:
    return cleaned_data[FEATURE_COLS]
```

```py
# driver.py
from hamilton import driver
import transforms

dr = driver.Driver({"source_path": "data.csv"}, transforms)
result = dr.execute(["feature_matrix"])
# Hamilton infers: source_path -> raw_data -> cleaned_data -> feature_matrix
```

**Key takeaway:** Hamilton's "function name \= node name, parameter name \= edge source" convention is extremely elegant for data transformation pipelines. However, it's poorly suited for workflow orchestration where you need explicit control over branching, joining, and iteration patterns. Metaflow's `self.next()` is more expressive for these cases.

*Sources: [Hamilton Introduction](https://blog.dagworks.io/p/functions-dags-introducing-hamilton-a-microframework-for-dataframe-generation-more-8e34b84efc1d), [Hamilton GitHub](https://github.com/DAGWorks-Inc/hamilton)*

### Summary of Approaches

| Framework | Graph Definition | Dynamic Construction | Edge Specification | Inspectable Before Run |
| :---- | :---- | :---- | :---- | :---- |
| **Metaflow** | Class with `@step` methods | Not supported (today) | `self.next()` in function body | Yes (AST) |
| **Airflow** | Imperative Python | Native (it's always imperative) | `>>` operator / `set_downstream()` | Yes |
| **Prefect** | Python function calls | Native (it's always dynamic) | Implicit (data flow) | No |
| **Dagster** | `@graph` decorator or `GraphDefinition` | `GraphDefinition` constructor | Call syntax or `DependencyDefinition` dict | Yes |
| **Hamilton** | Module-level functions | Module composition | Function parameter names | Yes |

---

## **Core Design Principle: Decoupling Functions from Topology**

Before diving into API proposals, we establish the fundamental principle that shapes all of them. This is the single most important design decision.

### The Principle

**A step's function body defines *what it computes*. The graph defines *where it sits*.**

Today these two concerns are tangled: `self.next()` inside the function body is the only way to specify edges. We propose cleanly separating them:

- **Topology** (edges, splits, joins, foreach) is defined by the **graph mutation API**
- **Computation** (the function body) is defined by a **plain callable**
- `self.next()` in a function body becomes **optional** \-- a convenience default, not a requirement

### Why This Matters

This separation makes step functions **independently reusable**. A function that trains a model can be plugged into different flows, in different positions, without modification. It also opens the door to future extensions like using a flow itself as a step (subflows), since the callable's internal structure doesn't need to know about its external wiring.

**A note on data flow:** This proposal decouples *control flow* (graph topology) from step functions, but *data flow* remains implicit through `self`. Steps communicate by setting attributes on `self` (e.g., `self.data = ...`) which downstream steps read \-- a convention that ties step implementations to a shared namespace. Full independence would require explicit data flow wiring (declaring which outputs of step A feed into which inputs of step B). This is out of scope here, but the separation of control flow is a prerequisite: once the graph is externally defined, explicit data flow becomes a natural next step. Subflows would similarly need data flow wiring to connect the inner flow's inputs/outputs to the outer flow's artifacts.

### The `self.next()` Resolution Rules

When building the graph, edge information can come from two sources: the function body (`self.next()`) and the mutation API (`set_next`, `insert_after`, etc.). We need unambiguous rules for how they interact.

**Rule 1: Mutator-defined edges take precedence.** If a mutator has set the outgoing edges for a step (via `set_next`, `insert_after`, etc.), those edges are authoritative. Any `self.next()` in the function body is ignored for graph construction purposes.

**Rule 2: `self.next()` in the body is the fallback default.** If no mutator has modified the edges for a step, `self.next()` in the function body is parsed via AST (as today) and used to determine the edges.

**Rule 3: Steps without `self.next()` are valid if edges are defined externally.** A step function with no `self.next()` call is allowed, provided its outgoing edges are defined by the mutation API. If a step has neither `self.next()` nor mutator-defined edges, graph validation raises an error (except for the `end` step which has no outgoing edges).

**Rule 4: At runtime, `self.next()` is a no-op when edges are externally defined.** When the runtime encounters a step whose edges were set by the mutation layer, the transition happens automatically when the function returns. If the function body calls `self.next()`, it is silently ignored (the mutation-defined edges win). If the function body does *not* call `self.next()`, the transition still happens correctly.

This means a step function like:

```py
def train_model(self):
    self.model = fit(self.data)
```

is a perfectly valid step \-- no `@step` decorator needed, no `self.next()` needed. The mutation API handles registration and wiring.

### What `add_step` Accepts

`add_step` should accept **any callable** with the proper calling convention:

- `def my_step(self)` \-- standard step
- `def my_join(self, inputs)` \-- join step (receives inputs from branches)

It does NOT require:

- The `@step` decorator (the mutation API applies the `is_step` marker internally)
- A `self.next()` call (edges are defined by the graph, not the body)
- Inspectable source code (edges come from metadata, not AST)

If the callable happens to have `@step` and/or `self.next()` already (e.g., reusing an existing step definition), those are respected as defaults per the rules above.

### Implications for the Runtime

The `FlowSpec.next()` method needs modification:

```
# Current behavior:
#   1. Parse self.next() arguments
#   2. Validate against static graph
#   3. Set self._transition

# New behavior:
#   1. Check if step has mutator-defined edges
#   2. If yes: self._transition is pre-set; self.next() calls are no-ops
#   3. If no: current behavior (parse arguments, validate, set transition)
```

The graph construction in `FlowGraph._create_nodes()` also needs modification:

```
# Current behavior:
#   1. inspect.getsourcelines(func) -> source
#   2. ast.parse(source) -> find self.next() -> determine edges

# New behavior:
#   1. Check if func has mutator-defined edge metadata
#   2. If yes: use metadata to build DAGNode (skip AST)
#   3. If no: current behavior (AST parsing)
```

---

## **Design Proposals**

All proposals share these principles:

- Operations happen within `FlowMutator.pre_mutate()` / `FlowMutator.mutate()` callbacks
- The `MutableFlow` and `MutableStep` remain the primary interfaces
- The `_init_graph()` rebuild happens automatically after mutations complete
- `self.next()` is optional; the graph is the source of truth for topology
- `add_step` accepts any callable (plain function, `@step`\-decorated, or with decorators already attached)

### Proposal A: Edge-Centric API

**Philosophy:** Treat the graph as a set of edges. Provide low-level primitives to manipulate edges directly. The user has full control but must manage consistency.

```py
class MutableStep:
    # --- New methods ---

    @property
    def next_steps(self) -> List[str]:
        """Return the names of steps this step transitions to."""

    def set_next(self, *steps: str, foreach: str = None,
                 condition: str = None) -> None:
        """Replace this step's outgoing edges entirely.

        Mirrors the self.next() API:
          step.set_next("step_b")                           # linear
          step.set_next("step_b", "step_c")                 # split
          step.set_next("step_b", foreach="items")          # foreach
          step.set_next({"a": "step_b", "b": "step_c"},    # switch
                        condition="choice")
        """

class MutableFlow:
    # --- New methods ---

    def add_step(self, name: str, body: Callable, decorators=None) -> MutableStep:
        """Add a new step to the flow. Does NOT connect it to anything.

        The `body` callable becomes the step function. It must accept
        `self` (and optionally `inputs` for joins). It does NOT need
        @step or self.next() -- edges are defined via set_next().
        """

    def remove_step(self, name: str) -> None:
        """Remove a step and all its incoming/outgoing edges."""
```

**Example: Insert a step between `start` and `process`**

```py
class AddLogging(FlowMutator):
    def pre_mutate(self, flow):
        def log_entry(self):
            print("Starting pipeline")

        flow.add_step("log_entry", log_entry)
        flow.start.set_next("log_entry")
        flow.log_entry.set_next("process")
```

**Example: Build a flow from scratch**

```py
@MakeFlow(config)
class MyFlow(FlowSpec):
    pass

class MakeFlow(FlowMutator):
    def init(self, config):
        self.config = config

    def pre_mutate(self, flow):
        def start(self):
            self.data = load(self.config)

        def process(self):
            self.result = transform(self.data)

        def end(self):
            save(self.result)

        flow.add_step("start", start)
        flow.add_step("process", process)
        flow.add_step("end", end)

        flow.start.set_next("process")
        flow.process.set_next("end")
```

**Pros:**

- Maximum flexibility \-- can express any graph transformation
- Mirrors `self.next()` semantics users already know (`set_next` uses same signature)
- Low-level primitives compose well
- Clean separation: functions have no topology, edges are explicit

**Cons:**

- User must manage graph consistency (dangling edges, orphaned steps)
- Verbose for common "insert between" operations
- `set_next` replaces all outgoing edges \-- no way to surgically add/remove a single edge without knowing the full set (see discussion below)

**Note on `add_next` / `remove_next`:** We initially considered finer-grained edge operations (`add_next(step_name)` and `remove_next(step_name)`). However, these have significant ergonomic problems:

- **`add_next` implies structural changes:** Adding a second outgoing edge converts a linear step into a split, which requires a corresponding join node. The user would need to simultaneously create and wire the join. This is not a single-edge concern \-- it's a graph-topology concern.
- **`remove_next` is ambiguous for switches:** If step A routes to B or C based on a condition, removing the edge to B doesn't just delete an edge \-- it changes the semantics of the condition. Should the condition be removed? Should B's case route elsewhere?

For these reasons, `set_next` (which replaces all edges atomically) is safer as the low-level primitive. Higher-level operations like `add_branch` (Proposal B) handle the structural implications explicitly.

### Proposal B: Positional Step Insertion API

**Philosophy:** Provide high-level operations that handle edge rewiring automatically. Common operations (insert before/after, replace) are first-class. Less common ones (arbitrary edge manipulation) are done through lower-level escape hatches.

```py
class MutableStep:
    # --- New methods ---

    def insert_after(self, name: str, body: Callable, decorators=None) -> MutableStep:
        """Insert a new step after this one.

        If this step has a single successor B:
          Before: self -> B
          After:  self -> name -> B

        If this step has multiple successors (split):
          Raises an error -- use insert_before on the target instead,
          or use add_branch.
        """

    def insert_before(self, name: str, body: Callable, decorators=None) -> MutableStep:
        """Insert a new step before this one.

        All predecessors of this step now point to the new step.
          Before: A -> self, B -> self
          After:  A -> name, B -> name, name -> self
        """

    def add_branch(self, name: str, body: Callable, decorators=None) -> MutableStep:
        """Add a new branch after a split step.

        This step must be a split. The new branch is added as an
        additional successor, and the matching join is updated.
          Before: self -> [B, C] -> join
          After:  self -> [B, C, name] -> join
        """

class MutableFlow:
    def add_step(self, name: str, body: Callable, decorators=None) -> MutableStep:
        """Add an unconnected step. Must be wired with set_next."""

    def remove_step(self, name: str, reconnect: bool = True) -> None:
        """Remove a step.

        If reconnect=True and the step has one predecessor A and one
        successor B: reconnects A -> B.
        If reconnect=True and the topology is ambiguous: raises an error.
        If reconnect=False: leaves dangling edges (caller must fix).
        """
```

**Example: Insert a step between `start` and `process`**

```py
class AddLogging(FlowMutator):
    def pre_mutate(self, flow):
        def log_entry(self):
            print("Starting pipeline")

        flow.start.insert_after("log_entry", log_entry)
        # start -> log_entry -> process (edges rewired automatically)
```

**Example: Add a validation branch to an existing split**

```py
class AddValidation(FlowMutator):
    def pre_mutate(self, flow):
        def validate(self):
            assert self.data is not None

        flow.split_step.add_branch("validate", validate)
        # split -> [branch_a, branch_b, validate] -> join (auto-wired)
```

**Pros:**

- Common operations are one-liners
- Edge consistency is maintained automatically
- Intuitive mental model: "insert after this step"
- Less error-prone than raw edge manipulation
- `add_branch` handles the split/join structural implications automatically

**Cons:**

- Limited expressiveness for complex graph transformations
- `insert_after` on a split raises an error \-- must use `add_branch` or `insert_before`

### Proposal C: Declarative Graph Builder

**Philosophy:** Instead of mutating an existing graph, provide a builder that constructs the graph declaratively. Steps are defined separately from their wiring.

```py
class MutableFlow:
    def build_graph(self) -> GraphBuilder:
        """Return a builder pre-populated with existing steps and edges."""

class GraphBuilder:
    def add_step(self, name: str, body: Callable, decorators=None) -> "GraphBuilder":
        """Add a step definition."""

    def remove_step(self, name: str) -> "GraphBuilder":
        """Remove a step definition."""

    def connect(self, source: str, *targets: str,
                foreach: str = None, condition: str = None) -> "GraphBuilder":
        """Define an edge from source to target(s)."""

    def disconnect(self, source: str, target: str = None) -> "GraphBuilder":
        """Remove edge(s) from source (all edges if target is None)."""

    def apply(self) -> None:
        """Finalize the graph. Validates and commits changes."""
```

**Example: Build a flow from config**

```py
class MakeFlow(FlowMutator):
    def init(self, config):
        self.config = config

    def pre_mutate(self, flow):
        builder = flow.build_graph()

        # Define steps
        for step_def in self.config["steps"]:
            builder.add_step(step_def["name"], step_def["func"])

        # Wire edges
        for edge in self.config["edges"]:
            builder.connect(edge["from"], *edge["to"],
                            foreach=edge.get("foreach"))

        builder.apply()
```

**Example: Modify existing graph**

```py
class InsertMiddleware(FlowMutator):
    def pre_mutate(self, flow):
        builder = flow.build_graph()
        builder.add_step("middleware", middleware_fn)
        builder.disconnect("start")
        builder.connect("start", "middleware")
        builder.connect("middleware", "process")
        builder.apply()
```

**Pros:**

- Clean separation of step definitions from topology
- Batch validation on `apply()` catches consistency issues
- Natural for "build from scratch" use case
- Inspired by Dagster's `GraphDefinition` but more Pythonic

**Cons:**

- Verbose for simple insertion
- Two-phase (build then apply) adds cognitive overhead
- The builder is a new concept users must learn
- Less intuitive than "insert after this step"

### Proposal D: Hybrid (Recommended)

**Philosophy:** Combine the intuitive high-level operations from Proposal B with the low-level edge control from Proposal A. Use a layered API where simple things are simple and complex things are possible.

#### *Layer 1: High-Level Step Operations (most common)*

```py
class MutableStep:
    @property
    def next_steps(self) -> List[str]:
        """Names of successor steps."""

    @property
    def previous_steps(self) -> List[str]:
        """Names of predecessor steps."""

    def insert_after(self, name: str, body: Callable, **kwargs) -> "MutableStep":
        """Insert step after this one. Handles edge rewiring.

        For linear steps (single successor B):
          self -> B  becomes  self -> name -> B

        For splits: raises error with guidance to use add_branch() or
        be explicit about which successor to insert before.
        """

    def insert_before(self, name: str, body: Callable, **kwargs) -> "MutableStep":
        """Insert step before this one. All predecessors rewired."""

class MutableFlow:
    def add_step(self, name: str, body: Callable, **kwargs) -> MutableStep:
        """Add step to flow, returns MutableStep for chaining.

        `body` can be:
        - A plain callable (def my_step(self): ...)
        - An @step-decorated function
        - A callable with decorators already attached

        Does NOT need self.next() -- edges are defined externally.
        """

    def remove_step(self, name: str, reconnect: bool = True) -> None:
        """Remove step, optionally reconnecting neighbors."""
```

#### *Layer 2: Edge Control (when you need precision)*

```py
class MutableStep:
    def set_next(self, *targets, foreach=None, condition=None) -> None:
        """Override this step's transitions. Same signature as self.next().

        Replaces all outgoing edges. For fine-grained split manipulation,
        use add_branch / remove_branch instead.
        """

    def add_branch(self, name: str, body: Callable, **kwargs) -> "MutableStep":
        """Add a new branch to a split step.

        Handles the structural implications: the new branch's outgoing
        edge is automatically set to the matching join node.
        """

    def remove_branch(self, step_name: str) -> None:
        """Remove a branch from a split step.

        If this leaves only one branch, the split collapses to linear.
        """
```

#### *Layer 3: Bulk Construction (for building from scratch)*

```py
class MutableFlow:
    def add_step(self, name: str, body: Callable, after: str = None,
                 before: str = None, **kwargs) -> MutableStep:
        """Add step with optional positioning.

        - after="step_a": insert after step_a (rewires edges)
        - before="step_b": insert before step_b (rewires edges)
        - neither: add unconnected (must wire with set_next)
        """
```

**Example: Simple insertion (Layer 1\)**

```py
class AddCache(FlowMutator):
    def pre_mutate(self, flow):
        def check_cache(self):
            self.cache_hit = lookup(self.key)

        flow.start.insert_after("check_cache", check_cache)
        # start -> check_cache -> process (auto-rewired)
```

**Example: Positional add (Layer 3\)**

```py
class AddCache(FlowMutator):
    def pre_mutate(self, flow):
        def check_cache(self):
            self.cache_hit = lookup(self.key)

        flow.add_step("check_cache", check_cache, after="start")
        # Equivalent to flow.start.insert_after(...)
```

**Example: Build a foreach pipeline from scratch (Layer 2\)**

```py
class MakeTrainingFlow(FlowMutator):
    def pre_mutate(self, flow):
        def start(self):
            self.models = ["rf", "xgb", "lgbm"]

        def train(self):
            self.result = fit(self.input)

        def join_results(self, inputs):
            self.results = [i.result for i in inputs]

        def end(self):
            print(f"Best: {best(self.results)}")

        flow.add_step("start", start)
        flow.add_step("train", train)
        flow.add_step("join_results", join_results)
        flow.add_step("end", end)

        flow.start.set_next("train", foreach="models")
        flow.train.set_next("join_results")
        flow.join_results.set_next("end")
```

**Example: Config-driven flow factory (the `@make_flow` dream)**

```py
@make_flow("pipeline.yaml")
class MyPipeline(FlowSpec):
    pass

# Where make_flow is:
class make_flow(FlowMutator):
    def init(self, config_path):
        self.config_path = config_path

    def pre_mutate(self, flow):
        config = yaml.safe_load(open(self.config_path))
        registry = get_step_registry()

        prev_step = None
        for i, step_conf in enumerate(config["steps"]):
            step_fn = registry[step_conf["type"]]
            name = step_conf.get("name", f"step_{i}")

            if prev_step is None:
                flow.add_step(name, step_fn)
            else:
                flow.add_step(name, step_fn, after=prev_step)
            prev_step = name
```

**Pros:**

- Progressive disclosure: simple tasks need simple code
- `insert_after`/`insert_before` handle 80% of use cases in one line
- `set_next` provides escape hatch for complex rewiring
- `add_step(after=...)` is convenient shorthand
- Step functions are plain callables \-- no `@step` or `self.next()` required
- Future-proof: subflows can be added as steps since callables are opaque

**Cons:**

- Larger API surface than any single proposal
- Must document which layer to use when

---

## **End-to-End Scenarios**

### Scenario 1: Config-Driven ML Pipeline

**User goal:** Define preprocessing, training, and evaluation steps in YAML. The mutator reads config and constructs the full flow.

```
# ipeline.yaml
steps:
  - name: load_data
    type: data_loader
    params: {source: "s3://bucket/data"}
  - name: preprocess
    type: sklearn_transformer
    params: {method: "standard_scaler"}
  - name: train
    type: model_trainer
    params: {model: "random_forest", cv_folds: 5}
  - name: evaluate
    type: evaluator
    params: {metrics: ["accuracy", "f1"]}
```

```py
@make_flow("pipeline.yaml")
class MLPipeline(FlowSpec):
    pass
```

### Scenario 2: Adding Observability to All Steps

**User goal:** Insert a logging/metrics step after every existing step.

```py
class AddObservability(FlowMutator):
    def pre_mutate(self, flow):
        for name, mutable_step in list(flow.steps):
            if name == "end":
                continue

            def observe(self):
                log_metrics(current.step_name, current.run_id)

            mutable_step.insert_after(f"{name}_observe", observe)
```

### Scenario 3: Conditional Feature Gating

**User goal:** If a config flag is set, insert extra validation between steps.

```py
class FeatureGate(FlowMutator):
    def pre_mutate(self, flow):
        if flow.config.get("enable_validation"):
            def validate(self):
                assert check_schema(self.data)

            flow.add_step("validate", validate, after="preprocess")
```

### Scenario 4: Reusing Step Libraries

**User goal:** Build flows from a library of reusable step functions.

```py
# step_library.py -- no Metaflow imports needed
def load_from_s3(self):
    import boto3
    self.data = boto3.client("s3").get_object(...)["Body"].read()

def standard_scale(self):
    from sklearn.preprocessing import StandardScaler
    self.data = StandardScaler().fit_transform(self.data)

def train_random_forest(self):
    from sklearn.ensemble import RandomForestClassifier
    self.model = RandomForestClassifier().fit(self.data, self.labels)
```

```py
# flow.py
from step_library import load_from_s3, standard_scale, train_random_forest

class BuildPipeline(FlowMutator):
    def pre_mutate(self, flow):
        flow.add_step("start", load_from_s3)
        flow.add_step("preprocess", standard_scale, after="start")
        flow.add_step("train", train_random_forest, after="preprocess")
        flow.add_step("end", lambda self: None, after="train")

@BuildPipeline
class MyFlow(FlowSpec):
    pass
```

This scenario highlights the power of decoupling: the step library has zero Metaflow dependencies. Functions are pure Python. The flow mutator handles all the wiring.

---

## **Resolved Design Decisions**

These questions were explored during research and have clear recommendations.

### 1\. Validation Timing

**Decision:** Lazy validation.

Allow arbitrary intermediate states during the mutation phase. Validate the full graph only when `_init_graph()` rebuilds after all mutations complete. This is consistent with how the current system works and enables complex multi-step construction without requiring a specific ordering of operations.

### 2\. Step Function Scope and `self`

**Decision:** It should "just work."

Step functions defined outside the class can reference `self.other_step` because by runtime, the function has been added to the class via `setattr`. The AST parser only needs the string name, which comes from metadata (not `self` resolution). For functions that don't call `self.next()` at all, this is a non-issue \-- the edges come entirely from the mutation API.

### 3\. Decorator Passthrough

**Decision:** No implicit inheritance. Explicit is better.

Inserted steps do NOT inherit decorators from surrounding steps. The `add_step` method accepts an optional `decorators` parameter. `add_step` should accept:

- A plain callable (no decorators)
- A callable that already has decorators attached (e.g., `@retry @step def ...`)
- A callable plus a separate `decorators` list

### 4\. What `add_step` Requires

**Decision:** Any callable with the right calling convention.

`add_step(name, body)` accepts any callable where `body` takes `self` (and optionally `inputs` for joins). It does NOT require `@step`, `self.next()`, or inspectable source. The mutation system applies the `is_step` marker internally and uses metadata for edges.

---

## **Open Questions**

### 1\. The AST Problem \-- Implementation Strategy

Metaflow currently parses `self.next()` from source code via AST. With the new system, we need to handle three cases:

| Case | Edge Source | AST Needed? |
| :---- | :---- | :---- |
| Traditional step (no mutator involvement) | `self.next()` in body | Yes (as today) |
| Step with mutator-overridden edges | Mutation metadata | No (skip AST for edges) |
| Dynamically added step (plain callable) | Mutation metadata | No (may not have parseable source) |

**Implementation approach:** Store edge metadata on the step function (e.g., `func._mf_edges`). Modify `FlowGraph._create_nodes()` to check for this metadata first. If present, build the DAGNode from metadata. If absent, fall back to AST parsing. This is fully backward-compatible.

**Open:** What exact data structure should `_mf_edges` use? It needs to encode:

- Target step names
- Edge type (linear, split, foreach, switch)
- Foreach parameter name (if applicable)
- Switch condition and case mapping (if applicable)

### 2\. Runtime Behavior of `self.next()` in Overridden Steps

When a step's edges have been overridden by a mutator, what happens if the function body still calls `self.next()`?

**Proposed behavior:**

```
def my_step(self):                    # Case 1: no self.next()
    self.data = process()             # -> transition happens automatically on return
                                      #    using mutator-defined edges

def my_step(self):                    # Case 2: self.next() matches mutator edges
    self.data = process()             # -> self.next() executes normally
    self.next(self.other_step)        #    (happens to match, no conflict)

def my_step(self):                    # Case 3: self.next() conflicts with mutator
    self.data = process()             # -> self.next() call is a no-op (ignored)
    self.next(self.wrong_step)        #    mutator edges win, transition on return
```

**Open:** Should Case 3 silently ignore or emit a warning? A warning would help users debug when a function body's `self.next()` doesn't match what the mutator set, but could be noisy when intentionally reusing functions in different positions. One option: warn only when the function was not added via `add_step` (i.e., it was a pre-existing step whose edges were later overridden).

### 3\. Interaction Between Multiple Mutators

When multiple `FlowMutator`s modify the same flow, they execute in decorator order (bottom-up, same as Python decorator semantics). If two mutators both try to modify edges on the same step, the last one wins.

**Open:** Should we provide a way to detect or prevent conflicts? For example, if mutator A sets `start.set_next("step_a")` and mutator B sets `start.set_next("step_b")`, B silently overwrites A. Should this warn? Or is "last writer wins" sufficient?

### 4\. Future Extension: Subflows

A later goal is to allow a flow to be used as a step (subflow). The design should not preclude this. Since `add_step` accepts any callable, a future extension could accept a FlowSpec class:

```py
flow.add_step("sub", SomeOtherFlow, after="start")
```

**Open:** Does this require any API changes now, or is the current callable-based design sufficient to support this later without breaking changes?

## **Notes**

* Remove start/end requirement:
  * Check the graph: start step is the one with no parents and end step is the one with no successor.
  * Consider pushing this info to the metadata so we don’t need to fetch the \_graph artifact or directly in the metadata service so it returns it directly.
* For packaging of additional step functions:
  * Enforce that they are backed by a file (so we can then make sure to include the file in the end package)
  * Take a string just like for user level decorators and look it up with load\_module
* Dataflow in the graph:
  * One mode: take the container: “the self”
  * Another mode: take arguments and add a mapping.
* Need good visualization for graphs that are created
  * This would not be for primarily for modifying graphs but for building new graph
  * We should still make sure we can get a visual of the graph that is executed
* Last step here would be `metaflow run <some file> –arguments-to-run` and there would be mappings between the files and the parser that generates the code.
  * Would it make sense to regenerate the code. What we could do instead is run the graph.
    * TL imports may be an issue.
    * We may need to support both because we need to execute what’s in the file. It’s not an explicit feature but people rely on it.

[image1]: <data:image/png;base64,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>
