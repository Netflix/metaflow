from kfp import dsl

from metaflow import FlowSpec, step, kfp, S3


def flip_coin_func(s3_root: str) -> str:
    """
    returns heads or tails
    """
    import random
    from metaflow import S3

    result = "tails" if random.randint(0, 2) == 0 else "heads"

    print("s3_root", s3_root)
    print("result", result)

    if result == "tails":
        with S3(s3root=s3_root) as s3:
            s3.put("my_result", result)

    return result


@dsl.graph_component
def my_recursive_component(s3_root):
    from kfp.components import func_to_container_op
    from kfp.dsl import ContainerOp

    flip_coin_op: ContainerOp = func_to_container_op(  # pylint: disable=E1102
        func=flip_coin_func, packages_to_install=["metaflow==2.2.4"]
    )(s3_root)

    with dsl.Condition(flip_coin_op.output == "heads"):
        my_recursive_component(s3_root)


class KfpGraphComponentFlow(FlowSpec):
    """
    A Flow that decorates a Metaflow Step with a KFP graph_component
    """

    @step
    def start(self):
        """
        kfp.preceding_component_inputs ["s3_root"] is passed to the graph_component because
        graph_components cannot return KFP outputs, and hence we use S3 flow root as the data
        passing mechanism.  See how the end step uses S3 to get the result.
        """
        with S3(run=self) as s3:
            self.s3_root = s3._s3root
            print("self.s3_root", self.s3_root)
            s3.put("start_key", "start_value")

        self.next(self.end)

    @kfp(
        preceding_component=my_recursive_component,
        preceding_component_inputs=["s3_root"],
    )
    @step
    def end(self):
        """
        S3 flow root path now has flip result in my_result
        """
        with S3(run=self) as s3:
            ret = s3.get("my_result")
            print("ret", ret)
            print("ret.text", ret.text)
            assert ret.text == "tails"
            assert s3.get("start_key").text == "start_value"


if __name__ == "__main__":
    KfpGraphComponentFlow()
