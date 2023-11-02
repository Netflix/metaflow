# Copyright 2020 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest


def _ignore_kfp_version_helper(spec):
    """Ignores kfp sdk versioning in command.

    Takes in a JSON input and ignores the kfp sdk versioning in command
    for comparison between compiled file and goldens.
    """
    pipeline_spec = spec['pipelineSpec'] if 'pipelineSpec' in spec else spec

    if 'executors' in pipeline_spec['deploymentSpec']:
        for executor in pipeline_spec['deploymentSpec']['executors']:
            pipeline_spec['deploymentSpec']['executors'][executor] = json.loads(
                re.sub(
                    "'kfp==(\d+).(\d+).(\d+)'", 'kfp',
                    json.dumps(pipeline_spec['deploymentSpec']['executors']
                               [executor])))
    return spec


class CompilerCliTests(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        return super().setUp()

    def _test_compile_py_to_json(
        self,
        file_base_name,
        additional_arguments=None,
        use_experimental=False,
    ):
        test_data_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        py_file = os.path.join(test_data_dir, '{}.py'.format(file_base_name))
        tmpdir = tempfile.mkdtemp()
        golden_compiled_file = os.path.join(test_data_dir,
                                            file_base_name + '.json')

        if additional_arguments is None:
            additional_arguments = []
        if use_experimental:
            additional_arguments.append('--use-experimental')

        def _compile(target_output_file: str):
            subprocess.check_call([
                'dsl-compile-v2', '--py', py_file, '--output',
                target_output_file
            ] + additional_arguments)

        def _load_compiled_file(filename: str):
            with open(filename, 'r') as f:
                contents = json.load(f)
                # Correct the sdkVersion
                pipeline_spec = contents[
                    'pipelineSpec'] if 'pipelineSpec' in contents else contents
                del pipeline_spec['sdkVersion']
                return _ignore_kfp_version_helper(contents)

        try:
            compiled_file = os.path.join(tmpdir,
                                         file_base_name + '-pipeline.json')
            _compile(target_output_file=compiled_file)

            golden = _load_compiled_file(golden_compiled_file)
            compiled = _load_compiled_file(compiled_file)

            # Devs can run the following command to update golden files:
            # UPDATE_GOLDENS=True python3 -m unittest kfp/v2/compiler_cli_tests/compiler_cli_tests.py
            # If UPDATE_GOLDENS=True, and the diff is
            # different, update the golden file and reload it.
            update_goldens = os.environ.get('UPDATE_GOLDENS', False)
            if golden != compiled and update_goldens:
                _compile(target_output_file=golden_compiled_file)
                golden = _load_compiled_file(golden_compiled_file)

            self.assertEqual(golden, compiled)
        finally:
            shutil.rmtree(tmpdir)

    def test_two_step_pipeline(self):
        self._test_compile_py_to_json(
            'two_step_pipeline',
            ['--pipeline-parameters', '{"text":"Hello KFP!"}'])

    def test_two_step_pipeline_experimental(self):
        self._test_compile_py_to_json(
            'experimental_two_step_pipeline', [
                '--pipeline-parameters',
                '{"text":"Hello KFP!"}',
            ],
            use_experimental=True)

    def test_pipeline_with_importer(self):
        self._test_compile_py_to_json('pipeline_with_importer')

    def test_pipeline_with_ontology(self):
        self._test_compile_py_to_json('pipeline_with_ontology')

    def test_pipeline_with_if_placeholder(self):
        self._test_compile_py_to_json('pipeline_with_if_placeholder')

    def test_pipeline_with_concat_placeholder(self):
        self._test_compile_py_to_json('pipeline_with_concat_placeholder')

    def test_pipeline_with_resource_spec(self):
        self._test_compile_py_to_json('pipeline_with_resource_spec')

    def test_pipeline_with_various_io_types(self):
        self._test_compile_py_to_json('pipeline_with_various_io_types')

    def test_pipeline_with_reused_component(self):
        self._test_compile_py_to_json('pipeline_with_reused_component')

    def test_pipeline_with_after(self):
        self._test_compile_py_to_json('pipeline_with_after')

    def test_pipeline_with_condition(self):
        self._test_compile_py_to_json('pipeline_with_condition')

    def test_pipeline_with_nested_conditions(self):
        self._test_compile_py_to_json('pipeline_with_nested_conditions')

    def test_pipeline_with_nested_conditions_yaml(self):
        self._test_compile_py_to_json('pipeline_with_nested_conditions_yaml')

    def test_pipeline_with_nested_conditions_yaml_experimental(self):
        self._test_compile_py_to_json(
            'experimental_pipeline_with_nested_conditions_yaml',
            use_experimental=True)

    def test_pipeline_with_loops(self):
        self._test_compile_py_to_json('pipeline_with_loops')

    def test_pipeline_with_loops_experimental(self):
        self._test_compile_py_to_json(
            'experimental_pipeline_with_loops', use_experimental=True)

    def test_pipeline_with_nested_loops(self):
        self._test_compile_py_to_json('pipeline_with_nested_loops')

    def test_pipeline_with_loops_and_conditions(self):
        self._test_compile_py_to_json('pipeline_with_loops_and_conditions')

    def test_pipeline_with_params_containing_format(self):
        self._test_compile_py_to_json('pipeline_with_params_containing_format')

    def test_lightweight_python_functions_v2_pipeline(self):
        self._test_compile_py_to_json(
            'lightweight_python_functions_v2_pipeline')

    def test_lightweight_python_functions_v2_with_outputs(self):
        self._test_compile_py_to_json(
            'lightweight_python_functions_v2_with_outputs')

    def test_xgboost_sample_pipeline(self):
        self._test_compile_py_to_json('xgboost_sample_pipeline')

    def test_pipeline_with_custom_job_spec(self):
        self._test_compile_py_to_json('pipeline_with_custom_job_spec')

    def test_pipeline_with_metrics_outputs(self):
        self._test_compile_py_to_json('pipeline_with_metrics_outputs')

    def test_pipeline_with_exit_handler(self):
        self._test_compile_py_to_json('pipeline_with_exit_handler')

    def test_pipeline_with_exit_handler_experimental(self):
        self._test_compile_py_to_json(
            'experimental_pipeline_with_exit_handler', use_experimental=True)

    def test_pipeline_with_env(self):
        self._test_compile_py_to_json('pipeline_with_env')

    def test_v2_component_with_optional_inputs(self):
        self._test_compile_py_to_json('v2_component_with_optional_inputs')

    def test_experimental_v2_component(self):
        self._test_compile_py_to_json(
            'experimental_v2_component', use_experimental=True)

    def test_pipeline_with_gcpc_types(self):
        self._test_compile_py_to_json('pipeline_with_gcpc_types')


if __name__ == '__main__':
    unittest.main()
