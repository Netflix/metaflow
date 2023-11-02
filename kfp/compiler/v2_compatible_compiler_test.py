# Copyright 2021 The Kubeflow Authors
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
"""Tests for v2-compatible compiled pipelines."""

import json
import os
import re
import tempfile
import unittest
from typing import Callable, Dict

import yaml
from kfp import compiler, components
from kfp import dsl as v1dsl
from kfp.v2 import dsl
from kfp.v2.dsl import Artifact, InputPath, OutputPath, component


@component
def preprocess(uri: str, some_int: int, output_parameter_one: OutputPath(int),
               output_dataset_one: OutputPath('Dataset')):
    """Dummy Preprocess Step."""
    with open(output_dataset_one, 'w') as f:
        f.write('Output dataset')
    with open(output_parameter_one, 'w') as f:
        f.write("{}".format(1234))


@component
def train(dataset: InputPath('Dataset'),
          model: OutputPath('Model'),
          num_steps: int = 100):
    """Dummy Training Step."""

    with open(dataset, 'r') as input_file:
        input_string = input_file.read()
        with open(model, 'w') as output_file:
            for i in range(num_steps):
                output_file.write("Step {}\n{}\n=====\n".format(
                    i, input_string))


class TestV2CompatibleModeCompiler(unittest.TestCase):

    def setUp(self) -> None:
        self.maxDiff = None
        return super().setUp()

    def _ignore_kfp_version_in_template(self, template):
        """Ignores kfp sdk versioning in container spec."""

    def _assert_compiled_pipeline_equals_golden(self,
                                                kfp_compiler: compiler.Compiler,
                                                pipeline_func: Callable,
                                                golden_yaml_filename: str):
        compiled_file = os.path.join(tempfile.mkdtemp(), 'workflow.yaml')
        kfp_compiler.compile(pipeline_func, package_path=compiled_file)

        test_data_dir = os.path.join(os.path.dirname(__file__), 'testdata')
        golden_file = os.path.join(test_data_dir, golden_yaml_filename)

        def _load_compiled_template(filename: str) -> Dict:
            with open(filename, 'r') as f:
                workflow = yaml.safe_load(f)

            del workflow['metadata']
            for template in workflow['spec']['templates']:
                template.pop('metadata', None)

                if 'initContainers' not in template:
                    continue
                # Strip off the launcher image label before comparison
                for initContainer in template['initContainers']:
                    initContainer['image'] = initContainer['image'].split(
                        ':')[0]

                if 'container' in template:
                    template['container'] = json.loads(
                        re.sub("'kfp==(\d+).(\d+).(\d+)'", 'kfp',
                               json.dumps(template['container'])))

            return workflow

        golden = _load_compiled_template(golden_file)
        compiled = _load_compiled_template(compiled_file)

        # Devs can run the following command to update golden files:
        # UPDATE_GOLDENS=True python3 -m unittest kfp/compiler/v2_compatible_compiler_test.py
        # If UPDATE_GOLDENS=True, and the diff is
        # different, update the golden file and reload it.
        update_goldens = os.environ.get('UPDATE_GOLDENS', False)
        if golden != compiled and update_goldens:
            kfp_compiler.compile(pipeline_func, package_path=golden_file)
            golden = _load_compiled_template(golden_file)

        self.assertDictEqual(golden, compiled)

    def test_two_step_pipeline(self):

        @dsl.pipeline(
            pipeline_root='gs://output-directory/v2-artifacts',
            name='my-test-pipeline')
        def v2_compatible_two_step_pipeline():
            preprocess_task = preprocess(uri='uri-to-import', some_int=12)
            train_task = train(
                num_steps=preprocess_task.outputs['output_parameter_one'],
                dataset=preprocess_task.outputs['output_dataset_one'])

        kfp_compiler = compiler.Compiler(
            mode=v1dsl.PipelineExecutionMode.V2_COMPATIBLE)
        self._assert_compiled_pipeline_equals_golden(
            kfp_compiler, v2_compatible_two_step_pipeline,
            'v2_compatible_two_step_pipeline.yaml')

    def test_custom_launcher(self):

        @dsl.pipeline(
            pipeline_root='gs://output-directory/v2-artifacts',
            name='my-test-pipeline-with-custom-launcher')
        def v2_compatible_two_step_pipeline():
            preprocess_task = preprocess(uri='uri-to-import', some_int=12)
            train_task = train(
                num_steps=preprocess_task.outputs['output_parameter_one'],
                dataset=preprocess_task.outputs['output_dataset_one'])

        kfp_compiler = compiler.Compiler(
            mode=v1dsl.PipelineExecutionMode.V2_COMPATIBLE,
            launcher_image='my-custom-image')
        self._assert_compiled_pipeline_equals_golden(
            kfp_compiler, v2_compatible_two_step_pipeline,
            'v2_compatible_two_step_pipeline_with_custom_launcher.yaml')

    def test_constructing_container_op_directly_should_error(self):

        @dsl.pipeline(name='test-pipeline')
        def my_pipeline():
            v1dsl.ContainerOp(
                name='comp1',
                image='gcr.io/dummy',
                command=['python', 'main.py'])

        with self.assertRaisesRegex(
                RuntimeError,
                'Constructing ContainerOp instances directly is deprecated and not '
                'supported when compiling to v2 \(using v2 compiler or v1 compiler '
                'with V2_COMPATIBLE or V2_ENGINE mode\).'):
            compiler.Compiler(
                mode=v1dsl.PipelineExecutionMode.V2_COMPATIBLE).compile(
                    pipeline_func=my_pipeline, package_path='result.json')

    def test_use_importer_should_error(self):

        @dsl.pipeline(name='test-pipeline')
        def my_pipeline():
            dsl.importer(artifact_uri='dummy', artifact_class=Artifact)

        with self.assertRaisesRegex(
                ValueError,
                'dsl.importer is not supported with v1 compiler.',
        ):
            compiler.Compiler(
                mode=v1dsl.PipelineExecutionMode.V2_COMPATIBLE).compile(
                    pipeline_func=my_pipeline, package_path='result.json')


if __name__ == '__main__':
    unittest.main()
