from typing import NamedTuple

from aip_kfp_sdk.components.component import kfp_component

from metaflow import FlowSpec, step, kfp, resources


@kfp_component(use_code_pickling=False)
def div_mod(
    dividend: int, divisor: int
) -> NamedTuple("result", [("quotient", int), ("remainder", int)]):
    print(f"dividend={dividend}, divisor={divisor}")
    return divmod(dividend, divisor)


class KfpFlow(FlowSpec):
    """
    Test adding a KFP Component and decorators
    """

    @resources(cpu=0.25, cpu_limit=5, memory=150, memory_limit="1G")
    @step
    def start(self):
        """
        All flows must have a step named 'start' that is the first step in the flow.
        """
        self.dividend = 26
        self.divisor = 7
        self.next(self.end)

    @kfp(
        preceding_component=div_mod,
        preceding_component_inputs="dividend divisor",
        preceding_component_outputs="quotient remainder",
    )
    @step
    def end(self):
        """
        Validate that the results of the preceding div_mod KFP step are bound
        to Metaflow state
        """
        print(f"quotient={type(self.quotient)}, remainder={type(self.remainder)}")
        print(f"quotient={self.quotient}, remainder={self.remainder}")
        assert int(self.quotient) == 3
        assert int(self.remainder) == 5


if __name__ == "__main__":
    KfpFlow()
