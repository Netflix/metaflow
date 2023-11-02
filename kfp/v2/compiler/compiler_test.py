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
import shutil
import tempfile
import unittest

from kfp import components
from kfp.v2 import compiler
from kfp.v2 import dsl
from kfp.dsl import types

VALID_PRODUCER_COMPONENT_SAMPLE = components.load_component_from_text("""
    name: producer
    inputs:
    - {name: input_param, type: String}
    outputs:
    - {name: output_model, type: Model}
    - {name: output_value, type: Integer}
    implementation:
      container:
        image: gcr.io/my-project/my-image:tag
        args:
        - {inputValue: input_param}
        - {outputPath: output_model}
        - {outputPath: output_value}
    """)


class CompilerTest(unittest.TestCase):

    def test_compile_simple_pipeline(self):

        tmpdir = tempfile.mkdtemp()
        try:
            producer_op = components.load_component_from_text("""
      name: producer
      inputs:
      - {name: input_param, type: String}
      outputs:
      - {name: output_model, type: Model}
      - {name: output_value, type: Integer}
      implementation:
        container:
          image: gcr.io/my-project/my-image:tag
          args:
          - {inputValue: input_param}
          - {outputPath: output_model}
          - {outputPath: output_value}
      """)

            consumer_op = components.load_component_from_text("""
      name: consumer
      inputs:
      - {name: input_model, type: Model}
      - {name: input_value, type: Integer}
      implementation:
        container:
          image: gcr.io/my-project/my-image:tag
          args:
          - {inputPath: input_model}
          - {inputValue: input_value}
      """)

            @dsl.pipeline(name='test-pipeline', pipeline_root='dummy_root')
            def simple_pipeline(pipeline_input: str = 'Hello KFP!'):
                producer = producer_op(input_param=pipeline_input)
                consumer = consumer_op(
                    input_model=producer.outputs['output_model'],
                    input_value=producer.outputs['output_value'])

            target_json_file = os.path.join(tmpdir, 'result.json')
            compiler.Compiler().compile(
                pipeline_func=simple_pipeline, package_path=target_json_file)

            self.assertTrue(os.path.exists(target_json_file))
        finally:
            shutil.rmtree(tmpdir)

    def test_compile_pipeline_with_dsl_graph_component_should_raise_error(self):

        with self.assertRaisesRegex(
                NotImplementedError,
                'dsl.graph_component is not yet supported in KFP v2 compiler.'):

            @dsl.component
            def flip_coin_op() -> str:
                import random
                result = 'heads' if random.randint(0, 1) == 0 else 'tails'
                return result

            @dsl.graph_component
            def flip_coin_graph_component():
                flip = flip_coin_op()
                with dsl.Condition(flip.output == 'heads'):
                    flip_coin_graph_component()

            @dsl.pipeline(name='test-pipeline', pipeline_root='dummy_root')
            def my_pipeline():
                flip_coin_graph_component()

            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_compile_pipeline_with_misused_inputvalue_should_raise_error(self):

        upstream_op = components.load_component_from_text("""
        name: upstream compoent
        outputs:
        - {name: model, type: Model}
        implementation:
          container:
            image: dummy
            args:
            - {outputPath: model}
        """)
        downstream_op = components.load_component_from_text("""
        name: compoent with misused placeholder
        inputs:
        - {name: model, type: Model}
        implementation:
          container:
            image: dummy
            args:
            - {inputValue: model}
        """)

        @dsl.pipeline(name='test-pipeline', pipeline_root='dummy_root')
        def my_pipeline():
            downstream_op(model=upstream_op().output)

        with self.assertRaisesRegex(
                TypeError,
                ' type "Model" cannot be paired with InputValuePlaceholder.'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_compile_pipeline_with_misused_inputpath_should_raise_error(self):

        component_op = components.load_component_from_text("""
        name: compoent with misused placeholder
        inputs:
        - {name: text, type: String}
        implementation:
          container:
            image: dummy
            args:
            - {inputPath: text}
        """)

        @dsl.pipeline(name='test-pipeline', pipeline_root='dummy_root')
        def my_pipeline(text: str):
            component_op(text=text)

        with self.assertRaisesRegex(
                TypeError,
                ' type "String" cannot be paired with InputPathPlaceholder.'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_compile_pipeline_with_missing_task_should_raise_error(self):

        @dsl.pipeline(name='test-pipeline', pipeline_root='dummy_root')
        def my_pipeline(text: str):
            pass

        with self.assertRaisesRegex(ValueError,
                                    'Task is missing from pipeline.'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_compile_pipeline_with_misused_inputuri_should_raise_error(self):

        component_op = components.load_component_from_text("""
        name: compoent with misused placeholder
        inputs:
        - {name: value, type: Float}
        implementation:
          container:
            image: dummy
            args:
            - {inputUri: value}
        """)

        @dsl.pipeline(name='test-pipeline', pipeline_root='dummy_root')
        def my_pipeline(value: float):
            component_op(value=value)

        with self.assertRaisesRegex(
                TypeError,
                ' type "Float" cannot be paired with InputUriPlaceholder.'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_compile_pipeline_with_misused_outputuri_should_raise_error(self):

        component_op = components.load_component_from_text("""
        name: compoent with misused placeholder
        outputs:
        - {name: value, type: Integer}
        implementation:
          container:
            image: dummy
            args:
            - {outputUri: value}
        """)

        @dsl.pipeline(name='test-pipeline', pipeline_root='dummy_root')
        def my_pipeline():
            component_op()

        with self.assertRaisesRegex(
                TypeError,
                ' type "Integer" cannot be paired with OutputUriPlaceholder.'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_compile_pipeline_with_invalid_name_should_raise_error(self):

        def my_pipeline():
            VALID_PRODUCER_COMPONENT_SAMPLE(input_param='input')

        with self.assertRaisesRegex(
                ValueError,
                'Invalid pipeline name: .*\nPlease specify a pipeline name that matches'
        ):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_set_pipeline_root_through_pipeline_decorator(self):

        tmpdir = tempfile.mkdtemp()
        try:

            @dsl.pipeline(name='test-pipeline', pipeline_root='gs://path')
            def my_pipeline():
                VALID_PRODUCER_COMPONENT_SAMPLE(input_param='input')

            target_json_file = os.path.join(tmpdir, 'result.json')
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path=target_json_file)

            self.assertTrue(os.path.exists(target_json_file))
            with open(target_json_file) as f:
                job_spec = json.load(f)
            self.assertEqual('gs://path',
                             job_spec['runtimeConfig']['gcsOutputDirectory'])
        finally:
            shutil.rmtree(tmpdir)

    def test_passing_string_parameter_to_artifact_should_error(self):

        component_op = components.load_component_from_text("""
      name: compoent
      inputs:
      - {name: some_input, type: , description: an uptyped input}
      implementation:
        container:
          image: dummy
          args:
          - {inputPath: some_input}
      """)

        @dsl.pipeline(name='test-pipeline', pipeline_root='gs://path')
        def my_pipeline(input1: str):
            component_op(some_input=input1)

        with self.assertRaisesRegex(
                TypeError,
                'Passing PipelineParam "input1" with type "String" \(as "Parameter"\) '
                'to component input "some_input" with type "None" \(as "Artifact"\) is '
                'incompatible. Please fix the type of the component input.'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_passing_missing_type_annotation_on_pipeline_input_should_error(
            self):

        @dsl.pipeline(name='test-pipeline', pipeline_root='gs://path')
        def my_pipeline(input1):
            pass

        with self.assertRaisesRegex(
                TypeError,
                'The pipeline argument \"input1\" is viewed as an artifact due '
                'to its type \"None\". And we currently do not support passing '
                'artifacts as pipeline inputs. Consider type annotating the '
                'argument with a primitive type, such as \"str\", \"int\", '
                '\"float\", \"bool\", \"dict\", and \"list\".'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='output.json')

    def test_passing_generic_artifact_to_input_expecting_concrete_artifact(
            self):

        producer_op1 = components.load_component_from_text("""
      name: producer compoent
      outputs:
      - {name: output, type: Artifact}
      implementation:
        container:
          image: dummy
          args:
          - {outputPath: output}
      """)

        @dsl.component
        def producer_op2(output: dsl.Output[dsl.Artifact]):
            pass

        consumer_op1 = components.load_component_from_text("""
      name: consumer compoent
      inputs:
      - {name: input, type: MyDataset}
      implementation:
        container:
          image: dummy
          args:
          - {inputPath: input}
      """)

        @dsl.component
        def consumer_op2(input: dsl.Input[dsl.Dataset]):
            pass

        @dsl.pipeline(name='test-pipeline')
        def my_pipeline():
            consumer_op1(producer_op1().output)
            consumer_op1(producer_op2().output)
            consumer_op2(producer_op1().output)
            consumer_op2(producer_op2().output)

        try:
            tmpdir = tempfile.mkdtemp()
            target_json_file = os.path.join(tmpdir, 'result.json')
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path=target_json_file)

            self.assertTrue(os.path.exists(target_json_file))
        finally:
            shutil.rmtree(tmpdir)

    def test_passing_concrete_artifact_to_input_expecting_generic_artifact(
            self):

        producer_op1 = components.load_component_from_text("""
      name: producer compoent
      outputs:
      - {name: output, type: Dataset}
      implementation:
        container:
          image: dummy
          args:
          - {outputPath: output}
      """)

        @dsl.component
        def producer_op2(output: dsl.Output[dsl.Model]):
            pass

        consumer_op1 = components.load_component_from_text("""
      name: consumer compoent
      inputs:
      - {name: input, type: Artifact}
      implementation:
        container:
          image: dummy
          args:
          - {inputPath: input}
      """)

        @dsl.component
        def consumer_op2(input: dsl.Input[dsl.Artifact]):
            pass

        @dsl.pipeline(name='test-pipeline')
        def my_pipeline():
            consumer_op1(producer_op1().output)
            consumer_op1(producer_op2().output)
            consumer_op2(producer_op1().output)
            consumer_op2(producer_op2().output)

        try:
            tmpdir = tempfile.mkdtemp()
            target_json_file = os.path.join(tmpdir, 'result.json')
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path=target_json_file)

            self.assertTrue(os.path.exists(target_json_file))
        finally:
            shutil.rmtree(tmpdir)

    def test_passing_arbitrary_artifact_to_input_expecting_concrete_artifact(
            self):

        producer_op1 = components.load_component_from_text("""
      name: producer compoent
      outputs:
      - {name: output, type: SomeArbitraryType}
      implementation:
        container:
          image: dummy
          args:
          - {outputPath: output}
      """)

        @dsl.component
        def consumer_op(input: dsl.Input[dsl.Dataset]):
            pass

        @dsl.pipeline(name='test-pipeline')
        def my_pipeline():
            consumer_op(producer_op1().output)
            consumer_op(producer_op2().output)

        with self.assertRaisesRegex(
                types.InconsistentTypeException,
                'Incompatible argument passed to the input "input" of component '
                '"Consumer op": Argument type "SomeArbitraryType" is incompatible with '
                'the input type "Dataset"'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='result.json')

    def test_constructing_container_op_directly_should_error(self):

        @dsl.pipeline(name='test-pipeline')
        def my_pipeline():
            dsl.ContainerOp(
                name='comp1',
                image='gcr.io/dummy',
                command=['python', 'main.py'])

        with self.assertRaisesRegex(
                RuntimeError,
                'Constructing ContainerOp instances directly is deprecated and not '
                'supported when compiling to v2 \(using v2 compiler or v1 compiler '
                'with V2_COMPATIBLE or V2_ENGINE mode\).'):
            compiler.Compiler().compile(
                pipeline_func=my_pipeline, package_path='result.json')


if __name__ == '__main__':
    unittest.main()
