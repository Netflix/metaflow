# Copyright 2018 The Kubeflow Authors
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

import requests
import textwrap
import unittest
from pathlib import Path
from unittest import mock

from .. import components as comp
from ..components._components import _resolve_command_line_and_paths
from ..components._yaml_utils import load_yaml
from ..components.structures import ComponentSpec


class LoadComponentTestCase(unittest.TestCase):

    def _test_load_component_from_file(self, component_path: str):
        task_factory1 = comp.load_component_from_file(component_path)

        arg1 = 3
        arg2 = 5
        task1 = task_factory1(arg1, arg2)

        self.assertEqual(task_factory1.__name__, 'Add')
        self.assertEqual(task_factory1.__doc__.strip(),
                         'Add\nReturns sum of two arguments')

        self.assertEqual(
            task1.component_ref.spec.implementation.container.image,
            'python:3.5')

        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)
        self.assertEqual(resolved_cmd.args[0], str(arg1))
        self.assertEqual(resolved_cmd.args[1], str(arg2))

    def test_load_component_from_yaml_file(self):
        component_path = Path(
            __file__).parent / 'test_data' / 'python_add.component.yaml'
        self._test_load_component_from_file(str(component_path))

    def test_load_component_from_zipped_yaml_file(self):
        component_path = Path(
            __file__).parent / 'test_data' / 'python_add.component.zip'
        self._test_load_component_from_file(str(component_path))

    def test_load_component_from_url(self):
        component_path = Path(
            __file__).parent / 'test_data' / 'python_add.component.yaml'
        component_url = 'https://raw.githubusercontent.com/some/repo/components/component_group/python_add/component.yaml'
        component_bytes = component_path.read_bytes()
        component_dict = load_yaml(component_bytes)

        def mock_response_factory(url, params=None, **kwargs):
            if url == component_url:
                response = requests.Response()
                response.url = component_url
                response.status_code = 200
                response._content = component_bytes
                return response
            raise RuntimeError('Unexpected URL "{}"'.format(url))

        with mock.patch('requests.get', mock_response_factory):
            task_factory1 = comp.load_component_from_url(component_url)

        self.assertEqual(
            task_factory1.__doc__,
            component_dict['name'] + '\n' + component_dict['description'])

        arg1 = 3
        arg2 = 5
        task1 = task_factory1(arg1, arg2)
        self.assertEqual(
            task1.component_ref.spec.implementation.container.image,
            component_dict['implementation']['container']['image'])

        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)
        self.assertEqual(resolved_cmd.args[0], str(arg1))
        self.assertEqual(resolved_cmd.args[1], str(arg2))

    def test_loading_minimal_component(self):
        component_text = '''\
implementation:
  container:
    image: busybox
'''
        component_dict = load_yaml(component_text)
        task_factory1 = comp.load_component(text=component_text)

        self.assertEqual(
            task_factory1.component_spec.implementation.container.image,
            component_dict['implementation']['container']['image'])

    def test_digest_of_loaded_component(self):
        component_text = textwrap.dedent('''\
            implementation:
              container:
                image: busybox
            ''')
        task_factory1 = comp.load_component_from_text(component_text)
        task1 = task_factory1()

        self.assertEqual(
            task1.component_ref.digest,
            '1ede211233e869581d098673962c2c1e8c1e4cebb7cf5d7332c2f73cb4900823')

    def test_accessing_component_spec_from_task_factory(self):
        component_text = '''\
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)

        actual_component_spec = task_factory1.component_spec
        actual_component_spec_dict = actual_component_spec.to_dict()
        expected_component_spec_dict = load_yaml(component_text)
        expected_component_spec = ComponentSpec.from_dict(
            expected_component_spec_dict)
        self.assertEqual(expected_component_spec_dict,
                         actual_component_spec_dict)
        self.assertEqual(expected_component_spec, task_factory1.component_spec)

    def test_fail_on_duplicate_input_names(self):
        component_text = '''\
inputs:
- {name: Data1}
- {name: Data1}
implementation:
  container:
    image: busybox
'''
        with self.assertRaises(ValueError):
            task_factory1 = comp.load_component_from_text(component_text)

    def test_fail_on_duplicate_output_names(self):
        component_text = '''\
outputs:
- {name: Data1}
- {name: Data1}
implementation:
  container:
    image: busybox
'''
        with self.assertRaises(ValueError):
            task_factory1 = comp.load_component_from_text(component_text)

    def test_handle_underscored_input_names(self):
        component_text = '''\
inputs:
- {name: Data}
- {name: _Data}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)

    def test_handle_underscored_output_names(self):
        component_text = '''\
outputs:
- {name: Data}
- {name: _Data}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)

    def test_handle_input_names_with_spaces(self):
        component_text = '''\
inputs:
- {name: Training data}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)

    def test_handle_output_names_with_spaces(self):
        component_text = '''\
outputs:
- {name: Training data}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)

    def test_handle_file_outputs_with_spaces(self):
        component_text = '''\
outputs:
- {name: Output data}
implementation:
  container:
    image: busybox
    fileOutputs:
      Output data: /outputs/output-data
'''
        task_factory1 = comp.load_component_from_text(component_text)

    def test_handle_similar_input_names(self):
        component_text = '''\
inputs:
- {name: Input 1}
- {name: Input_1}
- {name: Input-1}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)

    def test_conflicting_name_renaming_stability(self):
        # Checking that already pythonic input names are not renamed
        # Checking that renaming is deterministic
        component_text = textwrap.dedent('''\
            inputs:
            - {name: Input 1}
            - {name: Input_1}
            - {name: Input-1}
            - {name: input_1}  # Last in the list, but is pythonic, so it should not be renamed
            implementation:
              container:
                image: busybox
                command:
                - inputValue: Input 1
                - inputValue: Input_1
                - inputValue: Input-1
                - inputValue: input_1
            ''')
        task_factory1 = comp.load_component(text=component_text)
        task1 = task_factory1(
            input_1_2='value_1_2',
            input_1_3='value_1_3',
            input_1_4='value_1_4',
            input_1='value_1',  # Expecting this input not to be renamed
        )
        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)

        self.assertEqual(resolved_cmd.command,
                         ['value_1_2', 'value_1_3', 'value_1_4', 'value_1'])

    def test_handle_duplicate_input_output_names(self):
        component_text = '''\
inputs:
- {name: Data}
outputs:
- {name: Data}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)

    def test_fail_on_unknown_value_argument(self):
        component_text = '''\
inputs:
- {name: Data}
implementation:
  container:
    image: busybox
    args:
      - {inputValue: Wrong}
'''
        with self.assertRaises(TypeError):
            task_factory1 = comp.load_component_from_text(component_text)

    def test_fail_on_unknown_file_output(self):
        component_text = '''\
outputs:
- {name: Data}
implementation:
  container:
    image: busybox
    fileOutputs:
        Wrong: '/outputs/output.txt'
'''
        with self.assertRaises(TypeError):
            task_factory1 = comp.load_component_from_text(component_text)

    def test_load_component_fail_on_no_sources(self):
        with self.assertRaises(ValueError):
            comp.load_component()

    def test_load_component_fail_on_multiple_sources(self):
        with self.assertRaises(ValueError):
            comp.load_component(filename='', text='')

    def test_load_component_fail_on_none_arguments(self):
        with self.assertRaises(ValueError):
            comp.load_component(filename=None, url=None, text=None)

    def test_load_component_from_file_fail_on_none_arg(self):
        with self.assertRaises(TypeError):
            comp.load_component_from_file(None)

    def test_load_component_from_url_fail_on_none_arg(self):
        with self.assertRaises(TypeError):
            comp.load_component_from_url(None)

    def test_load_component_from_text_fail_on_none_arg(self):
        with self.assertRaises(TypeError):
            comp.load_component_from_text(None)

    def test_input_value_resolving(self):
        component_text = '''\
inputs:
- {name: Data}
implementation:
  container:
    image: busybox
    args:
      - --data
      - inputValue: Data
'''
        task_factory1 = comp.load_component(text=component_text)
        task1 = task_factory1('some-data')
        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)

        self.assertEqual(resolved_cmd.args, ['--data', 'some-data'])

    def test_automatic_output_resolving(self):
        component_text = '''\
outputs:
- {name: Data}
implementation:
  container:
    image: busybox
    args:
      - --output-data
      - {outputPath: Data}
'''
        task_factory1 = comp.load_component(text=component_text)
        task1 = task_factory1()
        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)

        self.assertEqual(len(resolved_cmd.args), 2)
        self.assertEqual(resolved_cmd.args[0], '--output-data')
        self.assertTrue(resolved_cmd.args[1].startswith('/'))

    def test_input_path_placeholder_with_constant_argument(self):
        component_text = '''\
inputs:
- {name: input 1}
implementation:
  container:
    image: busybox
    command:
      - --input-data
      - {inputPath: input 1}
'''
        task_factory1 = comp.load_component_from_text(component_text)
        task1 = task_factory1('Text')
        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)

        self.assertEqual(resolved_cmd.command,
                         ['--input-data', resolved_cmd.input_paths['input 1']])
        self.assertEqual(task1.arguments, {'input 1': 'Text'})

    def test_optional_inputs_reordering(self):
        """Tests optional input reordering.

        In python signature, optional arguments must come after the
        required arguments.
        """
        component_text = '''\
inputs:
- {name: in1}
- {name: in2, optional: true}
- {name: in3}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)
        import inspect
        signature = inspect.signature(task_factory1)
        actual_signature = list(signature.parameters.keys())
        self.assertSequenceEqual(actual_signature, ['in1', 'in3', 'in2'], str)

    def test_inputs_reordering_when_inputs_have_defaults(self):
        """Tests reordering of inputs with default values.

        In python signature, optional arguments must come after the
        required arguments.
        """
        component_text = '''\
inputs:
- {name: in1}
- {name: in2, default: val}
- {name: in3}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)
        import inspect
        signature = inspect.signature(task_factory1)
        actual_signature = list(signature.parameters.keys())
        self.assertSequenceEqual(actual_signature, ['in1', 'in3', 'in2'], str)

    def test_inputs_reordering_stability(self):
        """Tests input reordering stability.

        Required inputs and optional/default inputs should keep the
        ordering. In python signature, optional arguments must come
        after the required arguments.
        """
        component_text = '''\
inputs:
- {name: a1}
- {name: b1, default: val}
- {name: a2}
- {name: b2, optional: True}
- {name: a3}
- {name: b3, default: val}
- {name: a4}
- {name: b4, optional: True}
implementation:
  container:
    image: busybox
'''
        task_factory1 = comp.load_component_from_text(component_text)
        import inspect
        signature = inspect.signature(task_factory1)
        actual_signature = list(signature.parameters.keys())
        self.assertSequenceEqual(
            actual_signature, ['a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4'],
            str)

    def test_missing_optional_input_value_argument(self):
        """Missing optional inputs should resolve to nothing."""
        component_text = '''\
inputs:
- {name: input 1, optional: true}
implementation:
  container:
    image: busybox
    command:
      - a
      - {inputValue: input 1}
      - z
'''
        task_factory1 = comp.load_component_from_text(component_text)
        task1 = task_factory1()
        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)

        self.assertEqual(resolved_cmd.command, ['a', 'z'])

    def test_missing_optional_input_file_argument(self):
        """Missing optional inputs should resolve to nothing."""
        component_text = '''\
inputs:
- {name: input 1, optional: true}
implementation:
  container:
    image: busybox
    command:
      - a
      - {inputPath: input 1}
      - z
'''
        task_factory1 = comp.load_component_from_text(component_text)
        task1 = task_factory1()
        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)

        self.assertEqual(resolved_cmd.command, ['a', 'z'])

    def test_command_concat(self):
        component_text = '''\
inputs:
- {name: In1}
- {name: In2}
implementation:
  container:
    image: busybox
    args:
      - concat: [{inputValue: In1}, {inputValue: In2}]
'''
        task_factory1 = comp.load_component(text=component_text)
        task1 = task_factory1('some', 'data')
        resolved_cmd = _resolve_command_line_and_paths(task1.component_ref.spec,
                                                       task1.arguments)

        self.assertEqual(resolved_cmd.args, ['somedata'])

    def test_command_if_boolean_true_then_else(self):
        component_text = '''\
implementation:
  container:
    image: busybox
    args:
      - if:
          cond: true
          then: --true-arg
          else: --false-arg
'''
        task_factory1 = comp.load_component(text=component_text)
        task = task_factory1()
        resolved_cmd = _resolve_command_line_and_paths(task.component_ref.spec,
                                                       task.arguments)
        self.assertEqual(resolved_cmd.args, ['--true-arg'])

    def test_command_if_boolean_false_then_else(self):
        component_text = '''\
implementation:
  container:
    image: busybox
    args:
      - if:
          cond: false
          then: --true-arg
          else: --false-arg
'''
        task_factory1 = comp.load_component(text=component_text)
        task = task_factory1()
        resolved_cmd = _resolve_command_line_and_paths(task.component_ref.spec,
                                                       task.arguments)
        self.assertEqual(resolved_cmd.args, ['--false-arg'])

    def test_command_if_true_string_then_else(self):
        component_text = '''\
implementation:
  container:
    image: busybox
    args:
      - if:
          cond: 'true'
          then: --true-arg
          else: --false-arg
'''
        task_factory1 = comp.load_component(text=component_text)
        task = task_factory1()
        resolved_cmd = _resolve_command_line_and_paths(task.component_ref.spec,
                                                       task.arguments)
        self.assertEqual(resolved_cmd.args, ['--true-arg'])

    def test_command_if_false_string_then_else(self):
        component_text = '''\
implementation:
  container:
    image: busybox
    args:
      - if:
          cond: 'false'
          then: --true-arg
          else: --false-arg
'''
        task_factory1 = comp.load_component(text=component_text)

        task = task_factory1()
        resolved_cmd = _resolve_command_line_and_paths(task.component_ref.spec,
                                                       task.arguments)
        self.assertEqual(resolved_cmd.args, ['--false-arg'])

    def test_command_if_is_present_then(self):
        component_text = '''\
inputs:
- {name: In, optional: true}
implementation:
  container:
    image: busybox
    args:
      - if:
          cond: {isPresent: In}
          then: [--in, {inputValue: In}]
          #else: --no-in
'''
        task_factory1 = comp.load_component(text=component_text)

        task_then = task_factory1('data')
        resolved_cmd_then = _resolve_command_line_and_paths(
            task_then.component_ref.spec, task_then.arguments)
        self.assertEqual(resolved_cmd_then.args, ['--in', 'data'])

        task_else = task_factory1()
        resolved_cmd_else = _resolve_command_line_and_paths(
            task_else.component_ref.spec, task_else.arguments)
        self.assertEqual(resolved_cmd_else.args, [])

    def test_command_if_is_present_then_else(self):
        component_text = '''\
inputs:
- {name: In, optional: true}
implementation:
  container:
    image: busybox
    args:
      - if:
          cond: {isPresent: In}
          then: [--in, {inputValue: In}]
          else: --no-in
'''
        task_factory1 = comp.load_component(text=component_text)

        task_then = task_factory1('data')
        resolved_cmd_then = _resolve_command_line_and_paths(
            task_then.component_ref.spec, task_then.arguments)
        self.assertEqual(resolved_cmd_then.args, ['--in', 'data'])

        task_else = task_factory1()
        resolved_cmd_else = _resolve_command_line_and_paths(
            task_else.component_ref.spec, task_else.arguments)
        self.assertEqual(resolved_cmd_else.args, ['--no-in'])

    def test_command_if_input_value_then(self):
        component_text = '''\
inputs:
- {name: Do test, type: Boolean, optional: true}
- {name: Test data, type: Integer, optional: true}
- {name: Test parameter 1, optional: true}
implementation:
  container:
    image: busybox
    args:
      - if:
          cond: {inputValue: Do test}
          then: [--test-data, {inputValue: Test data}, --test-param1, {inputValue: Test parameter 1}]
'''
        task_factory1 = comp.load_component(text=component_text)

        task_then = task_factory1(True, 'test_data.txt', '42')
        resolved_cmd_then = _resolve_command_line_and_paths(
            task_then.component_ref.spec, task_then.arguments)
        self.assertEqual(
            resolved_cmd_then.args,
            ['--test-data', 'test_data.txt', '--test-param1', '42'])

        task_else = task_factory1()
        resolved_cmd_else = _resolve_command_line_and_paths(
            task_else.component_ref.spec, task_else.arguments)
        self.assertEqual(resolved_cmd_else.args, [])

    def test_handle_default_values_in_task_factory(self):
        component_text = '''\
inputs:
- {name: Data, default: '123'}
implementation:
  container:
    image: busybox
    args:
      - {inputValue: Data}
'''
        task_factory1 = comp.load_component_from_text(text=component_text)

        task1 = task_factory1()
        resolved_cmd1 = _resolve_command_line_and_paths(
            task1.component_ref.spec, task1.arguments)
        self.assertEqual(resolved_cmd1.args, ['123'])

        task2 = task_factory1('456')
        resolved_cmd2 = _resolve_command_line_and_paths(
            task2.component_ref.spec, task2.arguments)
        self.assertEqual(resolved_cmd2.args, ['456'])

    def test_check_task_spec_outputs_dictionary(self):
        component_text = '''\
outputs:
- {name: out 1}
- {name: out 2}
implementation:
  container:
    image: busybox
    command: [touch, {outputPath: out 1}, {outputPath: out 2}]
'''
        op = comp.load_component_from_text(component_text)

        task = op()

        self.assertEqual(list(task.outputs.keys()), ['out 1', 'out 2'])

    def test_check_task_object_no_output_attribute_when_0_outputs(self):
        component_text = textwrap.dedent(
            '''\
            implementation:
              container:
                image: busybox
                command: []
            ''',)

        op = comp.load_component_from_text(component_text)
        task = op()

        self.assertFalse(hasattr(task, 'output'))

    def test_check_task_object_has_output_attribute_when_1_output(self):
        component_text = textwrap.dedent(
            '''\
            outputs:
            - {name: out 1}
            implementation:
              container:
                image: busybox
                command: [touch, {outputPath: out 1}]
            ''',)

        op = comp.load_component_from_text(component_text)
        task = op()

        self.assertEqual(task.output.task_output.output_name, 'out 1')

    def test_check_task_object_no_output_attribute_when_multiple_outputs(self):
        component_text = textwrap.dedent(
            '''\
            outputs:
            - {name: out 1}
            - {name: out 2}
            implementation:
              container:
                image: busybox
                command: [touch, {outputPath: out 1}, {outputPath: out 2}]
            ''',)

        op = comp.load_component_from_text(component_text)
        task = op()

        self.assertFalse(hasattr(task, 'output'))

    def test_prevent_passing_unserializable_objects_as_argument(self):
        component_text = textwrap.dedent('''\
            inputs:
            - {name: input 1}
            - {name: input 2}
            implementation:
                container:
                    image: busybox
                    command:
                    - prog
                    - {inputValue: input 1}
                    - {inputPath: input 2}
            ''')
        component = comp.load_component_from_text(component_text)
        # Passing normal values to component
        task1 = component(input_1="value 1", input_2="value 2")
        # Passing unserializable values to component
        with self.assertRaises(TypeError):
            component(input_1=task1, input_2="value 2")
        with self.assertRaises(TypeError):
            component(input_1=open, input_2="value 2")
        with self.assertRaises(TypeError):
            component(input_1="value 1", input_2=task1)
        with self.assertRaises(TypeError):
            component(input_1="value 1", input_2=open)

    def test_check_type_validation_of_task_spec_outputs(self):
        producer_component_text = '''\
outputs:
- {name: out1, type: Integer}
- {name: out2, type: String}
implementation:
  container:
    image: busybox
    command: [touch, {outputPath: out1}, {outputPath: out2}]
'''
        consumer_component_text = '''\
inputs:
- {name: data, type: Integer}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: data}]
'''
        producer_op = comp.load_component_from_text(producer_component_text)
        consumer_op = comp.load_component_from_text(consumer_component_text)

        producer_task = producer_op()

        consumer_op(producer_task.outputs['out1'])
        consumer_op(producer_task.outputs['out2'].without_type())
        consumer_op(producer_task.outputs['out2'].with_type('Integer'))
        with self.assertRaises(TypeError):
            consumer_op(producer_task.outputs['out2'])

    def test_type_compatibility_check_for_simple_types(self):
        component_a = '''\
outputs:
  - {name: out1, type: custom_type}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: custom_type}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_type_compatibility_check_for_types_with_parameters(self):
        component_a = '''\
outputs:
  - {name: out1, type: {parametrized_type: {property_a: value_a, property_b: value_b}}}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: {parametrized_type: {property_a: value_a, property_b: value_b}}}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_type_compatibility_check_when_using_positional_arguments(self):
        """Tests that `op2(task1.output)` works as good as
        `op2(in1=task1.output)`"""
        component_a = '''\
outputs:
  - {name: out1, type: {parametrized_type: {property_a: value_a, property_b: value_b}}}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: {parametrized_type: {property_a: value_a, property_b: value_b}}}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        b_task = task_factory_b(a_task.outputs['out1'])

    def test_type_compatibility_check_when_input_type_is_missing(self):
        component_a = '''\
outputs:
  - {name: out1, type: custom_type}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_type_compatibility_check_when_argument_type_is_missing(self):
        component_a = '''\
outputs:
  - {name: out1}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: custom_type}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_fail_type_compatibility_check_when_simple_type_name_is_different(
            self):
        component_a = '''\
outputs:
  - {name: out1, type: type_A}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: type_Z}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        with self.assertRaises(TypeError):
            b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_fail_type_compatibility_check_when_parametrized_type_name_is_different(
            self):
        component_a = '''\
outputs:
  - {name: out1, type: {parametrized_type_A: {property_a: value_a}}}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: {parametrized_type_Z: {property_a: value_a}}}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        with self.assertRaises(TypeError):
            b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_fail_type_compatibility_check_when_type_property_value_is_different(
            self):
        component_a = '''\
outputs:
  - {name: out1, type: {parametrized_type: {property_a: value_a}}}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: {parametrized_type: {property_a: DIFFERENT VALUE}}}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        with self.assertRaises(TypeError):
            b_task = task_factory_b(in1=a_task.outputs['out1'])

    @unittest.skip('Type compatibility check currently works the opposite way')
    def test_type_compatibility_check_when_argument_type_has_extra_type_parameters(
            self):
        component_a = '''\
outputs:
  - {name: out1, type: {parametrized_type: {property_a: value_a, extra_property: extra_value}}}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: {parametrized_type: {property_a: value_a}}}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        b_task = task_factory_b(in1=a_task.outputs['out1'])

    @unittest.skip('Type compatibility check currently works the opposite way')
    def test_fail_type_compatibility_check_when_argument_type_has_missing_type_parameters(
            self):
        component_a = '''\
outputs:
  - {name: out1, type: {parametrized_type: {property_a: value_a}}}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: {parametrized_type: {property_a: value_a, property_b: value_b}}}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        with self.assertRaises(TypeError):
            b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_type_compatibility_check_not_failing_when_type_is_ignored(self):
        component_a = '''\
outputs:
  - {name: out1, type: type_A}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: type_Z}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        b_task = task_factory_b(in1=a_task.outputs['out1'].without_type())

    def test_type_compatibility_check_for_types_with_schema(self):
        component_a = '''\
outputs:
  - {name: out1, type: {GCSPath: {openapi_schema_validator: {type: string, pattern: "^gs://.*$" } }}}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: {GCSPath: {openapi_schema_validator: {type: string, pattern: "^gs://.*$" } }}}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()
        b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_fail_type_compatibility_check_for_types_with_different_schemas(
            self):
        component_a = '''\
outputs:
  - {name: out1, type: {GCSPath: {openapi_schema_validator: {type: string, pattern: AAA } }}}
implementation:
  container:
    image: busybox
    command: [bash, -c, 'mkdir -p "$(dirname "$0")"; date > "$0"', {outputPath: out1}]
'''
        component_b = '''\
inputs:
  - {name: in1, type: {GCSPath: {openapi_schema_validator: {type: string, pattern: ZZZ } }}}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: in1}]
'''
        task_factory_a = comp.load_component_from_text(component_a)
        task_factory_b = comp.load_component_from_text(component_b)
        a_task = task_factory_a()

        with self.assertRaises(TypeError):
            b_task = task_factory_b(in1=a_task.outputs['out1'])

    def test_container_component_without_command_should_warn(self):
        component_a = '''\
name: component without command
inputs:
  - {name: in1, type: String}
implementation:
  container:
    image: busybox
'''

        with self.assertWarnsRegex(
                FutureWarning,
                'Container component must specify command to be compatible with '
                'KFP v2 compatible mode and emissary executor'):
            task_factory_a = comp.load_component_from_text(component_a)


if __name__ == '__main__':
    unittest.main()
