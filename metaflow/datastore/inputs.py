class Inputs(object):
    """
    split: inputs.step_a.x inputs.step_b.x
    foreach: inputs[0].x
    both: (inp.x for inp in inputs)
    """

    def __init__(self, flows):
        # Sort by foreach index so that inputs arrive in the same order as the
        # original foreach list regardless of task completion order or datastore
        # retrieval order.
        def _foreach_index(flow):
            stack = getattr(flow, "_foreach_stack", None)
            if stack:
                return stack[-1].index
            return 0

        self.flows = sorted(flows, key=_foreach_index)
        for flow in self.flows:
            setattr(self, flow._current_step, flow)

    def __getitem__(self, idx):
        return self.flows[idx]

    def __iter__(self):
        return iter(self.flows)
