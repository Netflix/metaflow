class Inputs(object):
    """
    split: inputs.step_a.x inputs.step_b.x
    foreach: inputs[0].x
    both: (inp.x for inp in inputs)
    """

    def __init__(self, flows):
        self.flows = list(flows)
        self._sort_by_foreach_index()
        for flow in self.flows:
            setattr(self, flow._current_step, flow)

    def _sort_by_foreach_index(self):
        """Sort flows by foreach index if all flows have a _foreach_stack.

        For foreach joins, this ensures inputs[i] corresponds to the i-th
        split. For non-foreach joins (static splits), the order is left
        unchanged since there is no foreach index to sort by.
        """
        if not self.flows:
            return

        foreach_stacks = []
        for flow in self.flows:
            try:
                stack = flow._datastore["_foreach_stack"]
                if stack:
                    foreach_stacks.append(stack)
                else:
                    return  # empty stack — not a foreach join
            except (KeyError, AttributeError):
                return  # no foreach stack — not a foreach join

        # Sort by the index of the topmost foreach frame
        self.flows.sort(key=lambda f: f._datastore["_foreach_stack"][-1].index)

    def __getitem__(self, idx):
        return self.flows[idx]

    def __iter__(self):
        return iter(self.flows)
