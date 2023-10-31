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

import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, NamedTuple, Sequence

from .. import components as comp
from ..components import (InputBinaryFile, InputPath, InputTextFile,
                          OutputBinaryFile, OutputPath, OutputTextFile)
from ..components._components import _resolve_command_line_and_paths
from ..components.structures import InputSpec, OutputSpec


def add_two_numbers(a: float, b: float) -> float:
    """Returns sum of two arguments."""
    return a + b


@contextmanager
def components_local_output_dir_context(output_dir: str):
    old_dir = comp._components._outputs_dir
    try:
        comp._components._outputs_dir = output_dir
        yield output_dir
    finally:
        comp._components._outputs_dir = old_dir


@contextmanager
def components_override_input_output_dirs_context(inputs_dir: str,
                                                  outputs_dir: str):
    old_inputs_dir = comp._components._inputs_dir
    old_outputs_dir = comp._components._outputs_dir
    try:
        if inputs_dir:
            comp._components._inputs_dir = inputs_dir
        if outputs_dir:
            comp._components._outputs_dir = outputs_dir
        yield
    finally:
        comp._components._inputs_dir = old_inputs_dir
        comp._components._outputs_dir = old_outputs_dir


module_level_variable = 10


class ModuleLevelClass:

    def class_method(self, x):
        return x * module_level_variable


def module_func(a: float) -> float:
    return a * 5


def module_func_with_deps(a: float, b: float) -> float:
    return ModuleLevelClass().class_method(a) + module_func(b)


def dummy_in_0_out_0():
    pass


class PythonOpTestCase(unittest.TestCase):

    def helper_test_2_in_1_out_component_using_local_call(
            self, func, op, arguments=[3., 5.]):
        expected = func(arguments[0], arguments[1])
        if isinstance(expected, tuple):
            expected = expected[0]
        expected_str = str(expected)

        with tempfile.TemporaryDirectory() as temp_dir_name:
            with components_local_output_dir_context(temp_dir_name):
                task = op(arguments[0], arguments[1])
                resolved_cmd = _resolve_command_line_and_paths(
                    task.component_ref.spec,
                    task.arguments,
                )

            full_command = resolved_cmd.command + resolved_cmd.args
            # Setting the component python interpreter to the current one.
            # Otherwise the components are executed in different environment.
            # Some components (e.g. the ones that use code pickling) are sensitive to this.
            for i in range(2):
                if full_command[i] == 'python3':
                    full_command[i] = sys.executable
            subprocess.run(full_command, check=True)

            output_path = list(resolved_cmd.output_paths.values())[0]
            actual_str = Path(output_path).read_text()

        self.assertEqual(float(actual_str), float(expected_str))

    def helper_test_2_in_2_out_component_using_local_call(
            self, func, op, output_names):
        arg1 = float(3)
        arg2 = float(5)

        expected_tuple = func(arg1, arg2)
        expected1_str = str(expected_tuple[0])
        expected2_str = str(expected_tuple[1])

        with tempfile.TemporaryDirectory() as temp_dir_name:
            with components_local_output_dir_context(temp_dir_name):
                task = op(arg1, arg2)
                resolved_cmd = _resolve_command_line_and_paths(
                    task.component_ref.spec,
                    task.arguments,
                )

            full_command = resolved_cmd.command + resolved_cmd.args
            # Setting the component python interpreter to the current one.
            # Otherwise the components are executed in different environment.
            # Some components (e.g. the ones that use code pickling) are sensitive to this.
            for i in range(2):
                if full_command[i] == 'python3':
                    full_command[i] = sys.executable

            subprocess.run(full_command, check=True)

            (output_path1,
             output_path2) = (resolved_cmd.output_paths[output_names[0]],
                              resolved_cmd.output_paths[output_names[1]])
            actual1_str = Path(output_path1).read_text()
            actual2_str = Path(output_path2).read_text()

        self.assertEqual(float(actual1_str), float(expected1_str))
        self.assertEqual(float(actual2_str), float(expected2_str))

    def helper_test_component_created_from_func_using_local_call(
            self, func: Callable, arguments: dict):
        task_factory = comp.func_to_container_op(func)
        self.helper_test_component_against_func_using_local_call(
            func, task_factory, arguments)

    def helper_test_component_against_func_using_local_call(
            self, func: Callable, op: Callable, arguments: dict):
        # ! This function cannot be used when component has output types that use custom serialization since it will compare non-serialized function outputs with serialized component outputs.
        # Evaluating the function to get the expected output values
        expected_output_values_list = func(**arguments)
        if not isinstance(expected_output_values_list, Sequence) or isinstance(
                expected_output_values_list, str):
            expected_output_values_list = [str(expected_output_values_list)]
        expected_output_values_list = [
            str(value) for value in expected_output_values_list
        ]

        output_names = [output.name for output in op.component_spec.outputs]
        expected_output_values_dict = dict(
            zip(output_names, expected_output_values_list))

        self.helper_test_component_using_local_call(
            op, arguments, expected_output_values_dict)

    def helper_test_component_using_local_call(
            self,
            component_task_factory: Callable,
            arguments: dict = None,
            expected_output_values: dict = None):
        arguments = arguments or {}
        expected_output_values = expected_output_values or {}
        with tempfile.TemporaryDirectory() as temp_dir_name:
            # Creating task from the component.
            # We do it in a special context that allows us to control the output file locations.
            inputs_path = Path(temp_dir_name) / 'inputs'
            outputs_path = Path(temp_dir_name) / 'outputs'
            with components_override_input_output_dirs_context(
                    str(inputs_path), str(outputs_path)):
                task = component_task_factory(**arguments)
                resolved_cmd = _resolve_command_line_and_paths(
                    task.component_ref.spec,
                    task.arguments,
                )

            # Preparing input files
            for input_name, input_file_path in (resolved_cmd.input_paths or
                                                {}).items():
                Path(input_file_path).parent.mkdir(parents=True, exist_ok=True)
                Path(input_file_path).write_text(str(arguments[input_name]))

            # Constructing the full command-line from resolved command+args
            full_command = resolved_cmd.command + resolved_cmd.args
            # Setting the component python interpreter to the current one.
            # Otherwise the components are executed in different environment.
            # Some components (e.g. the ones that use code pickling) are sensitive to this.
            for i in range(2):
                if full_command[i] == 'python3':
                    full_command[i] = sys.executable

            # Executing the command-line locally
            subprocess.run(full_command, check=True)

            actual_output_values_dict = {
                output_name: Path(output_path).read_text() for output_name,
                output_path in resolved_cmd.output_paths.items()
            }

        self.assertDictEqual(actual_output_values_dict, expected_output_values)

    def test_func_to_container_op_local_call(self):
        func = add_two_numbers
        op = comp.func_to_container_op(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_func_to_container_op_output_component_file(self):
        func = add_two_numbers
        with tempfile.TemporaryDirectory() as temp_dir_name:
            component_path = str(Path(temp_dir_name) / 'component.yaml')
            comp.func_to_container_op(
                func, output_component_file=component_path)
            op = comp.load_component_from_file(component_path)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_func_to_component_file(self):
        func = add_two_numbers
        with tempfile.TemporaryDirectory() as temp_dir_name:
            component_path = str(Path(temp_dir_name) / 'component.yaml')
            comp._python_op.func_to_component_file(
                func, output_component_file=component_path)
            op = comp.load_component_from_file(component_path)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_indented_func_to_container_op_local_call(self):

        def add_two_numbers_indented(a: float, b: float) -> float:
            """Returns sum of two arguments."""
            return a + b

        func = add_two_numbers_indented
        op = comp.func_to_container_op(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_func_to_container_op_call_other_func(self):
        extra_variable = 10

        class ExtraClass:

            def class_method(self, x):
                return x * extra_variable

        def extra_func(a: float) -> float:
            return a * 5

        def main_func(a: float, b: float) -> float:
            return ExtraClass().class_method(a) + extra_func(b)

        func = main_func
        op = comp.func_to_container_op(func, use_code_pickling=True)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_func_to_container_op_check_nothing_extra_captured(self):

        def f1():
            pass

        def f2():
            pass

        def main_func(a: float, b: float) -> float:
            f1()
            try:
                eval('f2()')
            except:
                return a + b
            raise AssertionError(
                "f2 should not be captured, because it's not a dependency.")

        expected_func = lambda a, b: a + b
        op = comp.func_to_container_op(main_func, use_code_pickling=True)

        self.helper_test_2_in_1_out_component_using_local_call(
            expected_func, op)

    def test_func_to_container_op_call_other_func_global(self):
        func = module_func_with_deps
        op = comp.func_to_container_op(func, use_code_pickling=True)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_func_to_container_op_with_imported_func(self):
        from .test_data.module1 import \
            module_func_with_deps as module1_func_with_deps
        func = module1_func_with_deps
        op = comp.func_to_container_op(func, use_code_pickling=True)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_func_to_container_op_with_imported_func2(self):
        from .test_data import module1, module2_which_depends_on_module1
        func = module2_which_depends_on_module1.module2_func_with_deps
        op = comp.func_to_container_op(
            func,
            use_code_pickling=True,
            modules_to_capture=[
                module1.__name__,  # '*.components_tests.test_data.module1'
                func.
                __module__,  # '*.components_tests.test_data.module2_which_depends_on_module1'
            ])

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_func_to_container_op_multiple_named_typed_outputs(self):
        from typing import NamedTuple

        def add_multiply_two_numbers(
            a: float, b: float
        ) -> NamedTuple('DummyName', [('sum', float), ('product', float)]):
            """Returns sum and product of two arguments."""
            return (a + b, a * b)

        func = add_multiply_two_numbers
        op = comp.func_to_container_op(func)

        self.helper_test_2_in_2_out_component_using_local_call(
            func, op, output_names=['sum', 'product'])

    def test_extract_component_interface(self):
        from typing import NamedTuple

        def my_func(  # noqa: F722
            required_param,
            int_param: int = 42,
            float_param: float = 3.14,
            str_param: str = 'string',
            bool_param: bool = True,
            none_param=None,
            custom_type_param: 'Custom type' = None,
            custom_struct_type_param: {
                'CustomType': {
                    'param1': 'value1',
                    'param2': 'value2'
                }
            } = None,
        ) -> NamedTuple(
                'DummyName',
            [
                #('required_param',), # All typing.NamedTuple fields must have types
                ('int_param', int),
                ('float_param', float),
                ('str_param', str),
                ('bool_param', bool),
                #('custom_type_param', 'Custom type'), #SyntaxError: Forward reference must be an expression -- got 'Custom type'
                ('custom_type_param', 'CustomType'),
                #('custom_struct_type_param', {'CustomType': {'param1': 'value1', 'param2': 'value2'}}), # TypeError: NamedTuple('Name', [(f0, t0), (f1, t1), ...]); each t must be a type Got {'CustomType': {'param1': 'value1', 'param2': 'value2'}}
            ]):
            """Function docstring."""
            pass

        component_spec = comp._python_op._extract_component_interface(my_func)

        self.assertEqual(
            component_spec.inputs,
            [
                InputSpec(name='required_param'),
                InputSpec(
                    name='int_param',
                    type='Integer',
                    default='42',
                    optional=True),
                InputSpec(
                    name='float_param',
                    type='Float',
                    default='3.14',
                    optional=True),
                InputSpec(
                    name='str_param',
                    type='String',
                    default='string',
                    optional=True),
                InputSpec(
                    name='bool_param',
                    type='Boolean',
                    default='True',
                    optional=True),
                InputSpec(name='none_param',
                          optional=True),  # No default='None'
                InputSpec(
                    name='custom_type_param', type='Custom type',
                    optional=True),
                InputSpec(
                    name='custom_struct_type_param',
                    type={
                        'CustomType': {
                            'param1': 'value1',
                            'param2': 'value2'
                        }
                    },
                    optional=True),
            ])
        self.assertEqual(
            component_spec.outputs,
            [
                OutputSpec(name='int_param', type='Integer'),
                OutputSpec(name='float_param', type='Float'),
                OutputSpec(name='str_param', type='String'),
                OutputSpec(name='bool_param', type='Boolean'),
                #OutputSpec(name='custom_type_param', type='Custom type', default='None'),
                OutputSpec(name='custom_type_param', type='CustomType'),
                #OutputSpec(name='custom_struct_type_param', type={'CustomType': {'param1': 'value1', 'param2': 'value2'}}, optional=True),
            ])

        self.maxDiff = None
        self.assertDictEqual(
            component_spec.to_dict(),
            {
                'name':
                    'My func',
                'description':
                    'Function docstring.',
                'inputs': [
                    {
                        'name': 'required_param'
                    },
                    {
                        'name': 'int_param',
                        'type': 'Integer',
                        'default': '42',
                        'optional': True
                    },
                    {
                        'name': 'float_param',
                        'type': 'Float',
                        'default': '3.14',
                        'optional': True
                    },
                    {
                        'name': 'str_param',
                        'type': 'String',
                        'default': 'string',
                        'optional': True
                    },
                    {
                        'name': 'bool_param',
                        'type': 'Boolean',
                        'default': 'True',
                        'optional': True
                    },
                    {
                        'name': 'none_param',
                        'optional': True
                    },  # No default='None'
                    {
                        'name': 'custom_type_param',
                        'type': 'Custom type',
                        'optional': True
                    },
                    {
                        'name': 'custom_struct_type_param',
                        'type': {
                            'CustomType': {
                                'param1': 'value1',
                                'param2': 'value2'
                            }
                        },
                        'optional': True
                    },
                ],
                'outputs': [
                    {
                        'name': 'int_param',
                        'type': 'Integer'
                    },
                    {
                        'name': 'float_param',
                        'type': 'Float'
                    },
                    {
                        'name': 'str_param',
                        'type': 'String'
                    },
                    {
                        'name': 'bool_param',
                        'type': 'Boolean'
                    },
                    {
                        'name': 'custom_type_param',
                        'type': 'CustomType'
                    },
                    #{'name': 'custom_struct_type_param', 'type': {'CustomType': {'param1': 'value1', 'param2': 'value2'}}, 'optional': True},
                ]
            })

    @unittest.skip  #TODO: #Simplified multi-output syntax is not implemented yet
    def test_func_to_container_op_multiple_named_typed_outputs_using_list_syntax(
            self):

        def add_multiply_two_numbers(
                a: float, b: float) -> [('sum', float), ('product', float)]:
            """Returns sum and product of two arguments."""
            return (a + b, a * b)

        func = add_multiply_two_numbers
        op = comp.func_to_container_op(func)

        self.helper_test_2_in_2_out_component_using_local_call(
            func, op, output_names=['sum', 'product'])

    def test_func_to_container_op_named_typed_outputs_with_underscores(self):
        from typing import NamedTuple

        def add_two_numbers_name2(
                a: float,
                b: float) -> NamedTuple('DummyName', [('output_data', float)]):
            """Returns sum of two arguments."""
            return (a + b,)

        func = add_two_numbers_name2
        op = comp.func_to_container_op(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    @unittest.skip  #Python does not allow NamedTuple with spaces in names: ValueError: Type names and field names must be valid identifiers: 'Output data'
    def test_func_to_container_op_named_typed_outputs_with_spaces(self):
        from typing import NamedTuple

        def add_two_numbers_name3(
                a: float,
                b: float) -> NamedTuple('DummyName', [('Output data', float)]):
            """Returns sum of two arguments."""
            return (a + b,)

        func = add_two_numbers_name3
        op = comp.func_to_container_op(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_handling_same_input_output_names(self):
        from typing import NamedTuple

        def add_multiply_two_numbers(
                a: float, b: float
        ) -> NamedTuple('DummyName', [('a', float), ('b', float)]):
            """Returns sum and product of two arguments."""
            return (a + b, a * b)

        func = add_multiply_two_numbers
        op = comp.func_to_container_op(func)

        self.helper_test_2_in_2_out_component_using_local_call(
            func, op, output_names=['a', 'b'])

    def test_handling_same_input_default_output_names(self):

        def add_two_numbers_indented(a: float, Output: float) -> float:
            """Returns sum of two arguments."""
            return a + Output

        func = add_two_numbers_indented
        op = comp.func_to_container_op(func)

        self.helper_test_2_in_1_out_component_using_local_call(func, op)

    def test_legacy_python_component_name_description_overrides(self):
        # Deprecated feature

        expected_name = 'Sum component name'
        expected_description = 'Sum component description'
        expected_image = 'org/image'

        def add_two_numbers_decorated(
            a: float,
            b: float,
        ) -> float:
            """Returns sum of two arguments."""
            return a + b

        # Deprecated features
        add_two_numbers_decorated._component_human_name = expected_name
        add_two_numbers_decorated._component_description = expected_description
        add_two_numbers_decorated._component_base_image = expected_image

        func = add_two_numbers_decorated
        op = comp.func_to_container_op(func)

        component_spec = op.component_spec

        self.assertEqual(component_spec.name, expected_name)
        self.assertEqual(component_spec.description.strip(),
                         expected_description.strip())
        self.assertEqual(component_spec.implementation.container.image,
                         expected_image)

    def test_saving_default_values(self):
        from typing import NamedTuple

        def add_multiply_two_numbers(
            a: float = 3,
            b: float = 5
        ) -> NamedTuple('DummyName', [('sum', float), ('product', float)]):
            """Returns sum and product of two arguments."""
            return (a + b, a * b)

        func = add_multiply_two_numbers
        component_spec = comp._python_op._func_to_component_spec(func)

        self.assertEqual(component_spec.inputs[0].default, '3')
        self.assertEqual(component_spec.inputs[1].default, '5')

    def test_handling_of_descriptions(self):

        def pipeline(env_var: str,
                     secret_name: str,
                     secret_key: str = None) -> None:
            """Pipeline to Demonstrate Usage of Secret.

            Args:
                env_var: Name of the variable inside the Pod
                secret_name: Name of the Secret in the namespace
            """

        component_spec = comp._python_op._func_to_component_spec(pipeline)
        self.assertEqual(component_spec.description,
                         'Pipeline to Demonstrate Usage of Secret.')
        self.assertEqual(component_spec.inputs[0].description,
                         'Name of the variable inside the Pod')
        self.assertEqual(component_spec.inputs[1].description,
                         'Name of the Secret in the namespace')
        self.assertIsNone(component_spec.inputs[2].description)

    def test_handling_default_value_of_none(self):

        def assert_is_none(arg=None):
            assert arg is None

        func = assert_is_none
        op = comp.func_to_container_op(func)
        self.helper_test_component_using_local_call(op)

    def test_handling_complex_default_values(self):

        def assert_values_are_default(
                singleton_param=None,
                function_param=ascii,
                dict_param: dict = {'b': [2, 3, 4]},
                func_call_param='_'.join(['a', 'b', 'c']),
        ):
            assert singleton_param is None
            assert function_param is ascii
            assert dict_param == {'b': [2, 3, 4]}
            assert func_call_param == '_'.join(['a', 'b', 'c'])

        func = assert_values_are_default
        op = comp.func_to_container_op(func)
        self.helper_test_component_using_local_call(op)

    def test_handling_boolean_arguments(self):

        def assert_values_are_true_false(
            bool1: bool,
            bool2: bool,
        ) -> int:
            assert bool1 is True
            assert bool2 is False
            return 1

        func = assert_values_are_true_false
        op = comp.func_to_container_op(func)
        self.helper_test_2_in_1_out_component_using_local_call(
            func, op, arguments=[True, False])

    def test_handling_list_dict_arguments(self):

        def assert_values_are_same(
            list_param: list,
            dict_param: dict,
        ) -> int:
            import unittest
            unittest.TestCase().assertEqual(
                list_param,
                ["string", 1, 2.2, True, False, None, [3, 4], {
                    's': 5
                }])
            unittest.TestCase().assertEqual(
                dict_param, {
                    'str': "string",
                    'int': 1,
                    'float': 2.2,
                    'false': False,
                    'true': True,
                    'none': None,
                    'list': [3, 4],
                    'dict': {
                        's': 4
                    }
                })
            return 1

        # ! JSON map keys are always strings. Python converts all keys to strings without warnings
        func = assert_values_are_same
        op = comp.func_to_container_op(func)
        self.helper_test_2_in_1_out_component_using_local_call(
            func,
            op,
            arguments=[
                ["string", 1, 2.2, True, False, None, [3, 4], {
                    's': 5
                }],
                {
                    'str': "string",
                    'int': 1,
                    'float': 2.2,
                    'false': False,
                    'true': True,
                    'none': None,
                    'list': [3, 4],
                    'dict': {
                        's': 4
                    }
                },
            ])

    def test_fail_on_handling_list_arguments_containing_python_objects(self):
        """Checks that lists containing python objects not having .to_struct()
        raise error during serialization."""

        class MyClass:
            pass

        def consume_list(list_param: list,) -> int:
            return 1

        def consume_dict(dict_param: dict,) -> int:
            return 1

        list_op = comp.create_component_from_func(consume_list)
        dict_op = comp.create_component_from_func(consume_dict)

        with self.assertRaises(Exception):
            list_op([1, MyClass(), 3])

        with self.assertRaises(Exception):
            dict_op({'k1': MyClass()})

    def test_handling_list_arguments_containing_serializable_python_objects(
            self):
        """Checks that lists containing python objects with .to_struct() can be
        properly serialized."""

        class MyClass:

            def to_struct(self):
                return {'foo': [7, 42]}

        def assert_values_are_correct(
            list_param: list,
            dict_param: dict,
        ) -> int:
            import unittest
            unittest.TestCase().assertEqual(list_param,
                                            [1, {
                                                'foo': [7, 42]
                                            }, 3])
            unittest.TestCase().assertEqual(dict_param,
                                            {'k1': {
                                                'foo': [7, 42]
                                            }})
            return 1

        task_factory = comp.create_component_from_func(
            assert_values_are_correct)

        self.helper_test_component_using_local_call(
            task_factory,
            arguments=dict(
                list_param=[1, MyClass(), 3],
                dict_param={'k1': MyClass()},
            ),
            expected_output_values={'Output': '1'},
        )

    def test_handling_base64_pickle_arguments(self):

        def assert_values_are_same(
                obj1: 'Base64Pickle',  # noqa: F821
                obj2: 'Base64Pickle',  # noqa: F821
        ) -> int:
            import unittest
            unittest.TestCase().assertEqual(obj1['self'], obj1)
            unittest.TestCase().assertEqual(obj2, open)
            return 1

        func = assert_values_are_same
        op = comp.func_to_container_op(func)

        recursive_obj = {}
        recursive_obj['self'] = recursive_obj
        self.helper_test_2_in_1_out_component_using_local_call(
            func, op, arguments=[
                recursive_obj,
                open,
            ])

    def test_handling_list_dict_output_values(self):

        def produce_list() -> list:
            return ["string", 1, 2.2, True, False, None, [3, 4], {'s': 5}]

        # ! JSON map keys are always strings. Python converts all keys to strings without warnings
        task_factory = comp.func_to_container_op(produce_list)

        import json
        expected_output = json.dumps(
            ["string", 1, 2.2, True, False, None, [3, 4], {
                's': 5
            }])

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={},
            expected_output_values={'Output': expected_output})

    def test_input_path(self):

        def consume_file_path(number_file_path: InputPath(int)) -> int:
            with open(number_file_path) as f:
                string_data = f.read()
            return int(string_data)

        task_factory = comp.func_to_container_op(consume_file_path)

        self.assertEqual(task_factory.component_spec.inputs[0].type, 'Integer')

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={'number': "42"},
            expected_output_values={'Output': '42'})

    def test_input_text_file(self):

        def consume_file_path(number_file: InputTextFile(int)) -> int:
            string_data = number_file.read()
            assert isinstance(string_data, str)
            return int(string_data)

        task_factory = comp.func_to_container_op(consume_file_path)

        self.assertEqual(task_factory.component_spec.inputs[0].type, 'Integer')

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={'number': "42"},
            expected_output_values={'Output': '42'})

    def test_input_binary_file(self):

        def consume_file_path(number_file: InputBinaryFile(int)) -> int:
            bytes_data = number_file.read()
            assert isinstance(bytes_data, bytes)
            return int(bytes_data)

        task_factory = comp.func_to_container_op(consume_file_path)

        self.assertEqual(task_factory.component_spec.inputs[0].type, 'Integer')

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={'number': "42"},
            expected_output_values={'Output': '42'})

    def test_output_path(self):

        def write_to_file_path(number_file_path: OutputPath(int)):
            with open(number_file_path, 'w') as f:
                f.write(str(42))

        task_factory = comp.func_to_container_op(write_to_file_path)

        self.assertFalse(task_factory.component_spec.inputs)
        self.assertEqual(len(task_factory.component_spec.outputs), 1)
        self.assertEqual(task_factory.component_spec.outputs[0].type, 'Integer')

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={'number': '42'})

    def test_output_text_file(self):

        def write_to_file_path(number_file: OutputTextFile(int)):
            number_file.write(str(42))

        task_factory = comp.func_to_container_op(write_to_file_path)

        self.assertFalse(task_factory.component_spec.inputs)
        self.assertEqual(len(task_factory.component_spec.outputs), 1)
        self.assertEqual(task_factory.component_spec.outputs[0].type, 'Integer')

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={'number': '42'})

    def test_output_binary_file(self):

        def write_to_file_path(number_file: OutputBinaryFile(int)):
            number_file.write(b'42')

        task_factory = comp.func_to_container_op(write_to_file_path)

        self.assertFalse(task_factory.component_spec.inputs)
        self.assertEqual(len(task_factory.component_spec.outputs), 1)
        self.assertEqual(task_factory.component_spec.outputs[0].type, 'Integer')

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={'number': '42'})

    def test_output_path_plus_return_value(self):

        def write_to_file_path(number_file_path: OutputPath(int)) -> str:
            with open(number_file_path, 'w') as f:
                f.write(str(42))
            return 'Hello'

        task_factory = comp.func_to_container_op(write_to_file_path)

        self.assertFalse(task_factory.component_spec.inputs)
        self.assertEqual(len(task_factory.component_spec.outputs), 2)
        self.assertEqual(task_factory.component_spec.outputs[0].type, 'Integer')
        self.assertEqual(task_factory.component_spec.outputs[1].type, 'String')

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={},
            expected_output_values={
                'number': '42',
                'Output': 'Hello'
            })

    def test_all_data_passing_ways(self):

        def write_to_file_path(
            file_input1_path: InputPath(str),
            file_input2_file: InputTextFile(str),
            file_output1_path: OutputPath(str),
            file_output2_file: OutputTextFile(str),
            value_input1: str = 'foo',
            value_input2: str = 'foo',
        ) -> NamedTuple('Outputs', [
            ('return_output1', str),
            ('return_output2', str),
        ]):
            with open(file_input1_path, 'r') as file_input1_file:
                with open(file_output1_path, 'w') as file_output1_file:
                    file_output1_file.write(file_input1_file.read())

            file_output2_file.write(file_input2_file.read())

            return (value_input1, value_input2)

        task_factory = comp.func_to_container_op(write_to_file_path)

        self.assertEqual(
            set(input.name for input in task_factory.component_spec.inputs),
            {'file_input1', 'file_input2', 'value_input1', 'value_input2'})
        self.assertEqual(
            set(output.name for output in task_factory.component_spec.outputs),
            {
                'file_output1', 'file_output2', 'return_output1',
                'return_output2'
            })

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={
                'file_input1': 'file_input1_value',
                'file_input2': 'file_input2_value',
                'value_input1': 'value_input1_value',
                'value_input2': 'value_input2_value',
            },
            expected_output_values={
                'file_output1': 'file_input1_value',
                'file_output2': 'file_input2_value',
                'return_output1': 'value_input1_value',
                'return_output2': 'value_input2_value',
            },
        )

    def test_optional_input_path(self):

        def consume_file_path(number_file_path: InputPath(int) = None) -> int:
            result = -1
            if number_file_path:
                with open(number_file_path) as f:
                    string_data = f.read()
                    result = int(string_data)
            return result

        task_factory = comp.func_to_container_op(consume_file_path)

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={'Output': '-1'})

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={'number': "42"},
            expected_output_values={'Output': '42'})

    def test_fail_on_input_path_non_none_default(self):

        def read_from_file_path(
                file_path: InputPath(int) = '/tmp/something') -> str:
            return file_path

        with self.assertRaises(ValueError):
            task_factory = comp.func_to_container_op(read_from_file_path)

    def test_fail_on_output_path_default(self):

        def write_to_file_path(file_path: OutputPath(int) = None) -> str:
            return file_path

        with self.assertRaises(ValueError):
            task_factory = comp.func_to_container_op(write_to_file_path)

    def test_annotations_stripping(self):
        import collections
        import typing

        MyFuncOutputs = typing.NamedTuple('Outputs', [('sum', int),
                                                      ('product', int)])

        class CustomType1:
            pass

        def my_func(
            param1: CustomType1 = None,  # This caused failure previously
            param2: collections
            .OrderedDict = None,  # This caused failure previously
        ) -> MyFuncOutputs:  # This caused failure previously
            assert param1 == None
            assert param2 == None
            return (8, 15)

        task_factory = comp.create_component_from_func(my_func)

        self.helper_test_component_using_local_call(
            task_factory,
            arguments={},
            expected_output_values={
                'sum': '8',
                'product': '15'
            })

    def test_file_input_name_conversion(self):
        # Checking the input name conversion rules for file inputs:
        # For InputPath, the "_path" suffix is removed
        # For Input*, the "_file" suffix is removed

        def consume_file_path(
                number: int,
                number_1a_path: str,
                number_1b_file: str,
                number_1c_file_path: str,
                number_1d_path_file: str,
                number_2a_path: InputPath(str),
                number_2b_file: InputPath(str),
                number_2c_file_path: InputPath(str),
                number_2d_path_file: InputPath(str),
                number_3a_path: InputTextFile(str),
                number_3b_file: InputTextFile(str),
                number_3c_file_path: InputTextFile(str),
                number_3d_path_file: InputTextFile(str),
                number_4a_path: InputBinaryFile(str),
                number_4b_file: InputBinaryFile(str),
                number_4c_file_path: InputBinaryFile(str),
                number_4d_path_file: InputBinaryFile(str),
                output_number_2a_path: OutputPath(str),
                output_number_2b_file: OutputPath(str),
                output_number_2c_file_path: OutputPath(str),
                output_number_2d_path_file: OutputPath(str),
                output_number_3a_path: OutputTextFile(str),
                output_number_3b_file: OutputTextFile(str),
                output_number_3c_file_path: OutputTextFile(str),
                output_number_3d_path_file: OutputTextFile(str),
                output_number_4a_path: OutputBinaryFile(str),
                output_number_4b_file: OutputBinaryFile(str),
                output_number_4c_file_path: OutputBinaryFile(str),
                output_number_4d_path_file: OutputBinaryFile(str),
        ):
            pass

        task_factory = comp.func_to_container_op(consume_file_path)
        actual_input_names = [
            input.name for input in task_factory.component_spec.inputs
        ]
        actual_output_names = [
            output.name for output in task_factory.component_spec.outputs
        ]

        self.assertEqual([
            'number',
            'number_1a_path',
            'number_1b_file',
            'number_1c_file_path',
            'number_1d_path_file',
            'number_2a',
            'number_2b',
            'number_2c',
            'number_2d_path',
            'number_3a_path',
            'number_3b',
            'number_3c_file_path',
            'number_3d_path',
            'number_4a_path',
            'number_4b',
            'number_4c_file_path',
            'number_4d_path',
        ], actual_input_names)

        self.assertEqual([
            'output_number_2a',
            'output_number_2b',
            'output_number_2c',
            'output_number_2d_path',
            'output_number_3a_path',
            'output_number_3b',
            'output_number_3c_file_path',
            'output_number_3d_path',
            'output_number_4a_path',
            'output_number_4b',
            'output_number_4c_file_path',
            'output_number_4d_path',
        ], actual_output_names)

    def test_packages_to_install_feature(self):
        task_factory = comp.func_to_container_op(
            dummy_in_0_out_0, packages_to_install=['six', 'pip'])

        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={})

        task_factory2 = comp.func_to_container_op(
            dummy_in_0_out_0,
            packages_to_install=[
                'bad-package-0ee7cf93f396cd5072603dec154425cd53bf1c681c7c7605c60f8faf7799b901'
            ])
        with self.assertRaises(Exception):
            self.helper_test_component_using_local_call(
                task_factory2, arguments={}, expected_output_values={})

    def test_component_annotations(self):

        def some_func():
            pass

        annotations = {
            'key1': 'value1',
            'key2': 'value2',
        }
        task_factory = comp.create_component_from_func(
            some_func, annotations=annotations)
        component_spec = task_factory.component_spec
        self.assertEqual(component_spec.metadata.annotations, annotations)

    def test_code_with_escapes(self):

        def my_func():
            """Hello \n world."""

        task_factory = comp.create_component_from_func(my_func)
        self.helper_test_component_using_local_call(
            task_factory, arguments={}, expected_output_values={})

    def test_end_to_end_python_component_pipeline(self):
        #Defining the Python function
        def add(a: float, b: float) -> float:
            """Returns sum of two arguments."""
            return a + b

        with tempfile.TemporaryDirectory() as temp_dir_name:
            add_component_file = str(
                Path(temp_dir_name).joinpath('add.component.yaml'))

            #Converting the function to a component. Instantiate it to create a pipeline task (ContaineOp instance)
            add_op = comp.func_to_container_op(
                add,
                base_image='python:3.5',
                output_component_file=add_component_file)

            #Checking that the component artifact is usable:
            add_op2 = comp.load_component_from_file(add_component_file)

            #Building the pipeline
            def calc_pipeline(
                a1,
                a2='7',
                a3='17',
            ):
                task_1 = add_op(a1, a2)
                task_2 = add_op2(a1, a2)
                task_3 = add_op(task_1.outputs['Output'],
                                task_2.outputs['Output'])
                task_4 = add_op2(task_3.outputs['Output'], a3)

            #Instantiating the pipleine:
            calc_pipeline(42)

    def test_argument_serialization_failure(self):
        from typing import Sequence

        def my_func(args: Sequence[int]):
            print(args)

        task_factory = comp.create_component_from_func(my_func)
        with self.assertRaisesRegex(
                TypeError,
                'There are no registered serializers for type "(typing.)?Sequence(\[int\])?"'
        ):
            self.helper_test_component_using_local_call(
                task_factory, arguments={'args': [1, 2, 3]})

    def test_argument_serialization_success(self):
        from typing import List

        def my_func(args: List[int]):
            print(args)

        task_factory = comp.create_component_from_func(my_func)
        self.helper_test_component_using_local_call(
            task_factory, arguments={'args': [1, 2, 3]})


if __name__ == '__main__':
    unittest.main()
