from metaflow_test import FlowDefinition, steps


class BasicInclude(FlowDefinition):
    PRIORITY = 1
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
    INCLUDE_FILES = {
        "myfile_txt": {"default": "'./reg.txt'"},
        "myfile_utf8": {"default": "'./utf8.txt'", "encoding": "'utf8'"},
        "myfile_binary": {"default": "'./utf8.txt'", "is_text": False},
        "myfile_overriden": {"default": "'./reg.txt'"},
        "absent_file": {"required": False},
    }
    HEADER = """
import codecs
import os
os.environ['METAFLOW_RUN_MYFILE_OVERRIDEN'] = './override.txt'

with open('reg.txt', mode='w') as f:
    f.write("Regular Text File")
with codecs.open('utf8.txt', mode='w', encoding='utf8') as f:
    f.write(u"UTF Text File \u5e74")
with open('override.txt', mode='w') as f:
    f.write("Override Text File")
"""

    @steps(0, ["all"])
    def step_all(self):
        assert "Regular Text File" == self.myfile_txt
        assert "UTF Text File \u5e74" == self.myfile_utf8
        assert "UTF Text File \u5e74".encode(encoding="utf8") == self.myfile_binary
        assert "Override Text File" == self.myfile_overriden

        # Check that an absent file does not make things crash
        assert None == self.absent_file
        try:
            # Include files should be immutable
            self.myfile_txt = 5
            raise AssertionError("expected AttributeError but none was raised")
        except AttributeError:
            pass

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(step.name, "myfile_txt", "Regular Text File")
            checker.assert_artifact(step.name, "myfile_utf8", "UTF Text File \u5e74")
            checker.assert_artifact(
                step.name,
                "myfile_binary",
                "UTF Text File \u5e74".encode(encoding="utf8"),
            )
        checker.assert_artifact(step.name, "myfile_overriden", "Override Text File")
