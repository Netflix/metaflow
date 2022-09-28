from kfp.components import func_to_container_op

from metaflow import FlowSpec, step, kfp


@func_to_container_op
def my_hello_step_op_func(who: str) -> str:
    message = f"hello {who}"
    print(message)
    return message


class KfpHelloWorld(FlowSpec):
    """
    A Flow that decorates a Metaflow Step with a KFP component
    """

    @step
    def start(self):
        """
        kfp.preceding_component_inputs Flow state ["who"] is passed to the KFP component as arguments
        """
        self.who = "world"
        self.next(self.end)

    @kfp(
        preceding_component=my_hello_step_op_func,
        preceding_component_inputs=["who"],
        preceding_component_outputs=["message"],
    )
    @step
    def end(self):
        """
        kfp.preceding_component_outputs ["message"] is now available as Metaflow Flow state
        """
        print("message", self.message)


if __name__ == "__main__":
    KfpHelloWorld()
