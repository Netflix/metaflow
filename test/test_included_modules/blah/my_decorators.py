METAFLOW_PACKAGE = 1

import time
from typing import Any, Callable, Optional

from metaflow import StepDecorator, step_decorator
from metaflow.flowspec import FlowSpec


@step_decorator
def time_step(step_name, flow):
    start = time.time()
    to_raise = None
    try:
        yield
    except Exception as e:
        print(f"Error in step {step_name}: {e}")
        to_raise = e
    finally:
        end = time.time()
        flow.artifact_time = end - start
        print(f"Step {step_name} took {flow.artifact_time} seconds")
        if to_raise:
            raise to_raise


@step_decorator(name="print_me")
def some_weird_name(step_name, flow):
    print(f"Step {step_name} starting")
    yield
    print(f"Step {step_name} ending")


class MyComplexDecorator(StepDecorator):
    decorator_name = "my_complex_decorator"

    def init(self, **kwargs):
        self._excluded_step_names = kwargs.get("excluded_step_names", [])
        self._skip_steps = kwargs.get("skip_steps", [])

    def pre_step(
        self, step_name: str, flow: FlowSpec, inputs
    ) -> Callable[[FlowSpec, Any | None], Any] | None:
        if step_name in self._skip_steps:
            print(f"Skipping step {step_name}")
            self.skip_step = True
        elif step_name in self._excluded_step_names:
            print(f"Not doing complex stuff for step {step_name}")
        else:
            print("Doing something complex")

    def post_step(
        self, step_name: str, flow: FlowSpec, exception: Optional[Exception] = None
    ):
        if step_name in self._skip_steps:
            print(f"GAH -- should not be here; step {step_name} was skipped")
        elif step_name in self._excluded_step_names:
            print(f"Not doing complex stuff for step {step_name}")
        else:
            print("Done with complex stuff")
        if exception:
            raise exception
