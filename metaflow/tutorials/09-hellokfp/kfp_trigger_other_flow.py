from metaflow import FlowSpec, Parameter, Step, step
from metaflow.plugins.kfp import run_kubeflow_pipeline, to_metaflow_run_id


class KfpTriggerOtherFlow(FlowSpec):

    alpha: float = Parameter(
        "alpha",
        help="param with default",
        default=0.01,
    )

    @step
    def start(self):
        print("Run some batch training and scoring job here")
        self.beta: int = 20  # Potential output from modeling
        print(f"beta = {self.beta}")
        self.next(self.run_dependency_and_wait)

    @step
    def run_dependency_and_wait(self):
        # In this example ParameterFlow (see parameter_flow.py) is triggered.
        # It has been manually uploaded to AIP nonprod cluster.
        # The following info are known at upload time:
        kfp_pipeline_name: str = "aip-tutorial-parameter-flow"
        flow_name: str = "ParameterFlow"

        print("Launching a downstream pipeline and wait for its completion...")
        kfp_run_id: str = run_kubeflow_pipeline(
            pipeline_name=kfp_pipeline_name,
            kubeflow_namespace="aip-example-dev",
            parameters={
                "alpha": self.alpha,
                "beta": self.beta,
            },
            # By default this function triggers and return without waiting.
            # By specifying any wait_timeout larger than 0,
            # this function waits for downstream run and check success.
            wait_timeout=300,  # second
        )  # logs run_id and run_url automatically.

        print("Inspecting data of triggered run")

        # See https://docs.metaflow.org/metaflow/client for more run inspection methods
        metaflow_run_id: str = to_metaflow_run_id(kfp_run_id)
        start_step = Step(f"{flow_name}/{metaflow_run_id}/start")

        # Data `<var_name>` can be saved to artifact using `self.<var_name> = <value>`
        # Data of each step in triggered flow can then be retrieved as below.
        print("Value of start.alpha in triggered flow:", start_step.task.data.alpha)
        print("Value of start.beta in triggered flow:", start_step.task.data.beta)
        print("stdout of start step:\n", start_step.task.stdout)

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    KfpTriggerOtherFlow()
