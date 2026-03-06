import unittest
from metaflow.client.core import MetaflowData, Metaflow


class MockDataArtifact:
    def __init__(self, id):
        self.id = id


class TestMetaflowCompletions(unittest.TestCase):
    def test_metaflow_data_completions(self):
        artifacts = [MockDataArtifact("x"), MockDataArtifact("y")]
        data = MetaflowData(artifacts)

        dir_output = dir(data)
        self.assertIn("x", dir_output)
        self.assertIn("y", dir_output)

        keys = data._ipython_key_completions_()
        self.assertIn("x", keys)
        self.assertIn("y", keys)
        self.assertEqual(set(keys), {"x", "y"})

    def test_metaflow_completions(self):
        class MockFlow:
            def __init__(self, id):
                self.id = id

        # Override iterating over flows with a mock method
        # Because we only have mf instance, we can override '__iter__' on the class for the duration of the test,
        # or just set up the object so __iter__ works if it accesses self.

        # Metaflow __iter__ is not defined, but iteration works via returning self.flows.
        # Let's patch 'flows' property.
        class _MockMetaflow(Metaflow):
            def __iter__(self):
                yield MockFlow("flow_1")
                yield MockFlow("flow_2")

        mf_mock = _MockMetaflow.__new__(_MockMetaflow)
        keys = mf_mock._ipython_key_completions_()
        self.assertEqual(set(keys), {"flow_1", "flow_2"})

    def test_metaflow_data_dir_includes_artifacts(self):
        """Regression test for #1114: artifact names must appear in dir(MetaflowData).

        Before the fix, artifact names were NOT visible in dir(task.data),
        breaking IPython tab completion.
        """
        artifacts = [
            MockDataArtifact("my_var"),
            MockDataArtifact("name"),
            MockDataArtifact("my_other_var"),
        ]
        data = MetaflowData(artifacts)

        # Pre-fix: 'my_var', 'name', 'my_other_var' were NOT in dir(data)
        dir_output = dir(data)
        self.assertIn(
            "my_var",
            dir_output,
            "Artifact names must appear in dir() for IPython tab completion",
        )
        self.assertIn("name", dir_output)
        self.assertIn("my_other_var", dir_output)

    def test_metaflow_data_empty_artifacts(self):
        """Edge case: MetaflowData with no artifacts should still work."""
        data = MetaflowData([])

        dir_output = dir(data)
        keys = data._ipython_key_completions_()

        # Empty artifacts → no artifact keys, but standard attributes present
        self.assertEqual(keys, [])
        self.assertIn("__class__", dir_output)
        self.assertIn("__contains__", dir_output)

    def test_metaflow_completions_returns_empty_on_failure(self):
        """_ipython_key_completions_ must not crash IPython if iteration fails.

        Verifies graceful degradation: returns empty list instead of raising
        exception when metadata initialization fails.
        """

        class MockFlow:
            def __init__(self, id):
                self.id = id

        class _BrokenMetaflow(Metaflow):
            def __iter__(self):
                raise RuntimeError("metadata not initialized")

        mf = _BrokenMetaflow.__new__(_BrokenMetaflow)
        keys = mf._ipython_key_completions_()

        # Should return [] instead of crashing
        self.assertEqual(keys, [])


if __name__ == "__main__":
    unittest.main()
