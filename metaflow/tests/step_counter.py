from metaflow.decorators import StepDecorator


class StepCounter(StepDecorator):
    name = "step_counter"

    def task_post_step(
        self, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        if not hasattr(flow, "step_count"):
            flow.step_count = 0
        flow.step_count += 1
        print("step count: %d" % flow.step_count)
