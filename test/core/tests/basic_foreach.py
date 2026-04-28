from metaflow_test import FlowDefinition, ExpectationFailed, steps


class BasicForeach(FlowDefinition):
    PRIORITY = 0
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]

    @steps(0, ["foreach-split"], required=True)
    def split(self):
        self.my_index = None
        # Non-monotonic to catch foreach join ordering bugs
        self.arr = [
            26,
            5,
            10,
            15,
            25,
            11,
            22,
            6,
            19,
            12,
            16,
            9,
            28,
            14,
            24,
            20,
            30,
            1,
            13,
            18,
            2,
            17,
            21,
            3,
            29,
            4,
            27,
            31,
            8,
            23,
            0,
            7,
        ]

    @steps(0, ["foreach-inner"], required=True)
    def inner(self):
        # index must stay constant over multiple steps inside foreach
        if self.my_index is None:
            self.my_index = self.index
        assert self.my_index == self.index
        assert self.input == self.arr[self.index]
        self.my_input = self.input

    @steps(0, ["foreach-join"], required=True)
    def join(self, inputs):
        got = [inp.my_input for inp in inputs]
        assert (
            [
                26,
                5,
                10,
                15,
                25,
                11,
                22,
                6,
                19,
                12,
                16,
                9,
                28,
                14,
                24,
                20,
                30,
                1,
                13,
                18,
                2,
                17,
                21,
                3,
                29,
                4,
                27,
                31,
                8,
                23,
                0,
                7,
            ]
        ) == (got)

    @steps(1, ["all"])
    def step_all(self):
        pass
