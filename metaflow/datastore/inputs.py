class Inputs(object):
    """
    split: inputs.step_a.x inputs.step_b.x
    foreach: inputs[0].x
    both: (inp.x for inp in inputs)
    """

    def __init__(self, flows):
        # TODO sort by foreach index
        self.flows = list(flows)
        for flow in self.flows:
            setattr(self, flow._current_step, flow)

    def __getitem__(self, idx):
        return self.flows[idx]

    def __iter__(self):
        return iter(self.flows)
