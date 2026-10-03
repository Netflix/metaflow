from metaflow import FlowSpec, step


class SampleFlow(FlowSpec):
    @property
    def broken_prop(self):
        obj = {}
        return getattr(obj, "missing_attr")

    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_flowspec_getattr_property_attribute_error():
    flow = SampleFlow(use_cli=False)
    try:
        _ = flow.broken_prop
        assert False, "Expected AttributeError"
    except AttributeError as e:
        assert "raised an AttributeError during evaluation" in str(e)


def test_flowspec_getattr_missing_attribute_error():
    flow = SampleFlow(use_cli=False)
    try:
        _ = flow.non_existent_attr
        assert False, "Expected AttributeError"
    except AttributeError as e:
        assert "has no attribute 'non_existent_attr'" in str(e)
