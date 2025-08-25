from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


INLINE_SECRETS_VARS = [
    {
        "type": "inline",
        "id": "1",
        "options": {
            "env_vars": {
                "secret_1": "Pizza is a vegetable.",
                "SECRET_2": "How do eels reproduce?",
            },
        },
    }
]


class SecretsDecoratorTest(MetaflowTest):
    """
    Test that checks that the timeout decorator works as intended.
    """

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

    @tag("secrets(sources=%s)" % repr(INLINE_SECRETS_VARS))
    @steps(1, ["all"])
    def step_all(self):
        import os
        from metaflow import current

        if (
            self._graph[current.step_name].parallel_step
            and current.parallel.node_index != 0
            and os.environ.get("METAFLOW_RUNTIME_ENVIRONMENT", "local") == "local"
        ):
            # We don't check worker task secrets when there is a parallel step
            # run locally.
            # todo (future): support the case where secrets need to be passsed to the
            # control task in a parallel step when run locally.
            return

        assert os.environ.get("secret_1") == "Pizza is a vegetable."
        assert os.environ.get("SECRET_2") == "How do eels reproduce?"
