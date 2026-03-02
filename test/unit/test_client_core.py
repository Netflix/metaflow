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

        mf = Metaflow.__new__(Metaflow)
        
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

if __name__ == '__main__':
    unittest.main()
