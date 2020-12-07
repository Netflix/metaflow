from kfp.components import load_component_from_url

from metaflow import FlowSpec, step, kfp


URL = "https://raw.githubusercontent.com/kubeflow/pipelines/6931fe84f5b9e5fc9747ddc924890de41e4cd10e/sdk/python/kfp/v2/compiler_cli_tests/test_data/component_yaml/add_component.yaml"


class KfpLoadComponent(FlowSpec):
    """
    A Flow that decorates a Metaflow Step with a KFP component
    """

    @step
    def start(self):
        """
        kfp.preceding_component_inputs Flow state ["op1", "op2"] is passed to the KFP component as arguments
        """
        self.op1 = 2
        self.op2 = 3
        self.next(self.end)

    @kfp(
        preceding_component=load_component_from_url(URL),
        preceding_component_inputs="op1 op2",
        preceding_component_outputs="result",
    )
    @step
    def end(self):
        """
        kfp.preceding_component_outputs ["result"] is now available as Metaflow Flow state
        """
        print(f"result = {self.result}")
        assert int(self.result) == 5


if __name__ == "__main__":
    KfpLoadComponent()
