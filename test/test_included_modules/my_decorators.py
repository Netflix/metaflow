# METAFLOW_PACKAGE_POLICY = "include"

import time
from typing import Any, Callable, Optional

from metaflow import UserStepDecorator, user_step_decorator, StepMutator
from metaflow.flowspec import FlowSpec


def debug_deco_add(flow, deco_name, **kwargs):
    if not hasattr(flow, "user_added_step_decorators"):
        flow.user_added_step_decorators = []
    if kwargs:
        flow.user_added_step_decorators.append(f"{deco_name}({kwargs})")
        print(f"Running decorator {deco_name} with attributes {kwargs}")
    else:
        flow.user_added_step_decorators.append(deco_name)
        print(f"Running decorator {deco_name}")


@user_step_decorator
def retry(step_name, flow, inputs):
    debug_deco_add(flow, "retry")
    yield
    if hasattr(flow, "user_added_step_decorators"):
        delattr(flow, "user_added_step_decorators")  # Clean up after ourselves


@user_step_decorator
def time_step(step_name, flow, inputs):
    debug_deco_add(flow, "time_step")
    start = time.time()
    to_raise = None
    try:
        yield
        if hasattr(flow, "user_added_step_decorators"):
            delattr(flow, "user_added_step_decorators")  # Clean up after ourselves

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


class AddArgsDecorator(StepMutator):
    def init(self, *args, **kwargs):
        self.my_kwargs = kwargs

    def mutate(self, mutable_step):
        mutable_step.add_decorator(
            mutable_step.flow.cfg.args_decorator, deco_kwargs=self.my_kwargs
        )


@user_step_decorator
def with_args(step_name, flow, inputs, attributes):
    debug_deco_add(flow, "with_args", **attributes)
    yield
    if hasattr(flow, "user_added_step_decorators"):
        delattr(flow, "user_added_step_decorators")  # Clean up after ourselves


class SkipStep(UserStepDecorator):
    def init(self, *args, **kwargs):
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
            print(f"Doing something complex for step {step_name}")

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
