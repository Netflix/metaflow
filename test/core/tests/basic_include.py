from metaflow_test import MetaflowTest, ExpectationFailed, steps


class BasicIncludeTest(MetaflowTest):
    PRIORITY = 1
    INCLUDE_FILES = {
        'myfile_txt': {'default': "'./reg.txt'"},
        'myfile_utf8': {'default': "'./utf8.txt'", 'encoding': "'utf8'"},
        'myfile_binary': {'default': "'./utf8.txt'", 'is_text': False},
        'myfile_overriden': {'default': "'./reg.txt'"},
        'absent_file': {'required': False}
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

    @steps(0, ['all'])
    def step_all(self):
        assert_equals("Regular Text File", self.myfile_txt)
        assert_equals(u"UTF Text File \u5e74", self.myfile_utf8)
        assert_equals(
            u"UTF Text File \u5e74".encode(encoding='utf8'), self.myfile_binary)
        assert_equals("Override Text File", self.myfile_overriden)

        # Check that an absent file does not make things crash
        assert_equals(None, self.absent_file)
        try:
            # Include files should be immutable
            self.myfile_txt = 5
            raise ExpectationFailed(AttributeError, 'nothing')
        except AttributeError:
            pass

    def check_results(self, flow, checker):
        run = checker.get_run()
        if run is None:
            # CliChecker does not return a run object; we check to make sure
            # the returned value is the blob describing the artifact
            # (this may be improved in the future)
            for step in flow:
                checker.assert_artifact(
                    step.name,
                    'myfile_txt',
                    None,
                    fields={'type': 'uploader-v1',
                            'is_text': True,
                            'encoding': None})
                checker.assert_artifact(
                    step.name,
                    'myfile_utf8',
                    None,
                    fields={'type': 'uploader-v1',
                            'is_text': True,
                            'encoding': 'utf8'})
                checker.assert_artifact(
                    step.name,
                    'myfile_binary',
                    None,
                    fields={'type': 'uploader-v1',
                            'is_text': False,
                            'encoding': None})
                checker.assert_artifact(
                    step.name,
                    'myfile_overriden',
                    None,
                    fields={'type': 'uploader-v1',
                            'is_text': True,
                            'encoding': None})
        else:
            # In the case of the client, we check the value.
            for step in flow:
                checker.assert_artifact(step.name, 'myfile_txt',
                                        "Regular Text File")
                checker.assert_artifact(step.name, 'myfile_utf8',
                                        u"UTF Text File \u5e74")
                checker.assert_artifact(step.name, 'myfile_binary',
                                        u"UTF Text File \u5e74".encode(encoding='utf8'))
                checker.assert_artifact(step.name, 'myfile_overriden',
                                        "Override Text File")

