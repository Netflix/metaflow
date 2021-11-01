from metaflow_test import MetaflowTest, ExpectationFailed, steps


class LineageTest(MetaflowTest):
    PRIORITY = 1

    @steps(0, ["start"])
    def step_start(self):
        self.lineage = (self._current_step,)

    @steps(1, ["join"])
    def step_join(self):
        # we can't easily account for the number of foreach splits,
        # so we only care about unique lineages (hence set())
        self.lineage = (tuple(sorted({x.lineage for x in inputs})), self._current_step)

    @steps(2, ["all"])
    def step_all(self):
        self.lineage += (self._current_step,)

    def check_results(self, flow, checker):
        from collections import defaultdict

        join_sets = defaultdict(set)
        lineages = {}
        graph = flow._graph

        # traverse all paths from the start step to the end,
        # collect lineages on the way and finally compare them
        # to the lineages produced by the actual run
        def traverse(step, lineage):
            if graph[step].type == "join":
                join_sets[step].add(tuple(lineage))
                if len(join_sets[step]) < len(graph[step].in_funcs):
                    return
                else:
                    lineage = (tuple(sorted(join_sets[step])),)
            lineages[step] = lineage + (step,)
            for n in graph[step].out_funcs:
                traverse(n, lineage + (step,))

        traverse("start", ())

        for step in flow:
            checker.assert_artifact(step.name, "lineage", lineages[step.name])
