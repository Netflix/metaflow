# METAFLOW_PACKAGE = 1

import time
from typing import Any, Callable, Optional

from metaflow import UserStepDecorator, user_step_decorator, StepMutator
from metaflow.flowspec import FlowSpec


@user_step_decorator
def retry(step_name, flow, inputs):
    print("This is a user retry function")
    yield
    print("Didn't user retry")


@user_step_decorator
def time_step(step_name, flow, inputs):
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


class AddTimeStep(StepMutator):
    def mutate(self, mutable_step):
        mutable_step.add_decorator(time_step)


@user_step_decorator
def with_args(step_name, flow, inputs, attributes):
    print(f"Step {step_name} starting; decorator had {attributes}")
    yield
    print(f"Step {step_name} ending")


class MyComplexDecorator(UserStepDecorator):

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
            flow.did_skip = True
            print(f"Skipped step {step_name}")
        elif step_name in self._excluded_step_names:
            print(f"Not doing complex stuff for step {step_name}")
        else:
            print("Done with complex stuff")
        if exception:
            raise exception
