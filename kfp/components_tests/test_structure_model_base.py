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

import os
import sys
import unittest
from pathlib import Path

from typing import List, Dict, Union, Optional
from ..components.modelbase import ModelBase


class TestModel1(ModelBase):
    _serialized_names = {
        'prop_1': 'prop1',
        'prop_2': 'prop 2',
        'prop_3': '@@',
    }

    def __init__(
        self,
        prop_0: str,
        prop_1: Optional[str] = None,
        prop_2: Union[int, str, bool] = '',
        prop_3: 'TestModel1' = None,
        prop_4: Optional[Dict[str, 'TestModel1']] = None,
        prop_5: Optional[Union['TestModel1', List['TestModel1'],
                               Dict[str, 'TestModel1']]] = None,
        prop_6: Optional[Union[str, List, Dict]] = None,
    ):
        #print(locals())
        super().__init__(locals())


class StructureModelBaseTestCase(unittest.TestCase):

    def test_handle_type_check_for_simple_builtin(self):
        self.assertEqual(TestModel1(prop_0='value 0').prop_0, 'value 0')

        with self.assertRaises(TypeError):
            TestModel1(prop_0=1)

        with self.assertRaises(TypeError):
            TestModel1(prop_0=None)

        with self.assertRaises(TypeError):
            TestModel1(prop_0=TestModel1(prop_0='value 0'))

    def test_handle_type_check_for_optional_builtin(self):
        self.assertEqual(
            TestModel1(prop_0='', prop_1='value 1').prop_1, 'value 1')
        self.assertEqual(TestModel1(prop_0='', prop_1=None).prop_1, None)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_1=1)

        with self.assertRaises(TypeError):
            TestModel1(
                prop_0='', prop_1=TestModel1(prop_0='', prop_1='value 1'))

    def test_handle_type_check_for_union_builtin(self):
        self.assertEqual(
            TestModel1(prop_0='', prop_2='value 2').prop_2, 'value 2')
        self.assertEqual(TestModel1(prop_0='', prop_2=22).prop_2, 22)
        self.assertEqual(TestModel1(prop_0='', prop_2=True).prop_2, True)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_2=None)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_2=22.22)

        with self.assertRaises(TypeError):
            TestModel1(
                prop_0='', prop_2=TestModel1(prop_0='', prop_2='value 2'))

    def test_handle_type_check_for_class(self):
        val3 = TestModel1(prop_0='value 0')
        self.assertEqual(TestModel1(prop_0='', prop_3=val3).prop_3, val3)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_3=1)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_3='value 3')

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_3=[val3])

    def test_handle_type_check_for_dict_class(self):
        val4 = TestModel1(prop_0='value 0')
        self.assertEqual(
            TestModel1(prop_0='', prop_4={
                'key 4': val4
            }).prop_4['key 4'], val4)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_4=1)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_4='value 4')

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_4=[val4])

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_4={42: val4})

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_4={'key 4': [val4]})

    def test_handle_type_check_for_union_dict_class(self):
        val5 = TestModel1(prop_0='value 0')
        self.assertEqual(TestModel1(prop_0='', prop_5=val5).prop_5, val5)
        self.assertEqual(TestModel1(prop_0='', prop_5=[val5]).prop_5[0], val5)
        self.assertEqual(
            TestModel1(prop_0='', prop_5={
                'key 5': val5
            }).prop_5['key 5'], val5)
        self.assertEqual(TestModel1(prop_0='', prop_5=None).prop_5, None)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_5=1)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_5='value 5')

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_5={'key 5': 'value 5'})

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_5={42: val5})

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_5={'key 5': [val5]})

    def test_handle_type_check_for_open_generic_classes(self):
        val6 = TestModel1(prop_0='value 0')
        self.assertEqual(TestModel1(prop_0='', prop_6='val6').prop_6, 'val6')
        self.assertEqual(TestModel1(prop_0='', prop_6=[val6]).prop_6[0], val6)
        self.assertEqual(
            TestModel1(prop_0='', prop_6={
                'key 6': val6
            }).prop_6['key 6'], val6)
        self.assertEqual(TestModel1(prop_0='', prop_6=None).prop_6, None)

        with self.assertRaises(TypeError):
            TestModel1(prop_0='', prop_6=1)

    def test_handle_from_to_dict_for_simple_builtin(self):
        struct0 = {'prop_0': 'value 0'}
        obj0 = TestModel1.from_dict(struct0)
        self.assertEqual(obj0.prop_0, 'value 0')
        self.assertDictEqual(obj0.to_dict(), struct0)

        with self.assertRaises(AttributeError):  #TypeError:
            TestModel1.from_dict(None)

        with self.assertRaises(AttributeError):  #TypeError:
            TestModel1.from_dict('')

        with self.assertRaises(TypeError):
            TestModel1.from_dict({})

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop0': 'value 0'})

    def test_handle_from_to_dict_for_optional_builtin(self):
        struct11 = {'prop_0': '', 'prop1': 'value 1'}
        obj11 = TestModel1.from_dict(struct11)
        self.assertEqual(obj11.prop_1, struct11['prop1'])
        self.assertDictEqual(obj11.to_dict(), struct11)

        struct12 = {'prop_0': '', 'prop1': None}
        obj12 = TestModel1.from_dict(struct12)
        self.assertEqual(obj12.prop_1, None)
        self.assertDictEqual(obj12.to_dict(), {'prop_0': ''})

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop_0': '', 'prop 1': ''})

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop_0': '', 'prop1': 1})

    def test_handle_from_to_dict_for_union_builtin(self):
        struct21 = {'prop_0': '', 'prop 2': 'value 2'}
        obj21 = TestModel1.from_dict(struct21)
        self.assertEqual(obj21.prop_2, struct21['prop 2'])
        self.assertDictEqual(obj21.to_dict(), struct21)

        struct22 = {'prop_0': '', 'prop 2': 22}
        obj22 = TestModel1.from_dict(struct22)
        self.assertEqual(obj22.prop_2, struct22['prop 2'])
        self.assertDictEqual(obj22.to_dict(), struct22)

        struct23 = {'prop_0': '', 'prop 2': True}
        obj23 = TestModel1.from_dict(struct23)
        self.assertEqual(obj23.prop_2, struct23['prop 2'])
        self.assertDictEqual(obj23.to_dict(), struct23)

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop_0': 'ZZZ', 'prop 2': None})

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop_0': '', 'prop 2': 22.22})

    def test_handle_from_to_dict_for_class(self):
        val3 = TestModel1(prop_0='value 0')

        struct31 = {
            'prop_0': '',
            '@@': val3.to_dict()
        }  #{'prop_0': '', '@@': TestModel1(prop_0='value 0')} is also valid for from_dict, but this cannot happen when parsing for real
        obj31 = TestModel1.from_dict(struct31)
        self.assertEqual(obj31.prop_3, val3)
        self.assertDictEqual(obj31.to_dict(), struct31)

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop_0': '', '@@': 'value 3'})

    def test_handle_from_to_dict_for_dict_class(self):
        val4 = TestModel1(prop_0='value 0')

        struct41 = {'prop_0': '', 'prop_4': {'val 4': val4.to_dict()}}
        obj41 = TestModel1.from_dict(struct41)
        self.assertEqual(obj41.prop_4['val 4'], val4)
        self.assertDictEqual(obj41.to_dict(), struct41)

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop_0': '', 'prop_4': {44: val4.to_dict()}})

    def test_handle_from_to_dict_for_union_dict_class(self):
        val5 = TestModel1(prop_0='value 0')

        struct51 = {'prop_0': '', 'prop_5': val5.to_dict()}
        obj51 = TestModel1.from_dict(struct51)
        self.assertEqual(obj51.prop_5, val5)
        self.assertDictEqual(obj51.to_dict(), struct51)

        struct52 = {'prop_0': '', 'prop_5': [val5.to_dict()]}
        obj52 = TestModel1.from_dict(struct52)
        self.assertListEqual(obj52.prop_5, [val5])
        self.assertDictEqual(obj52.to_dict(), struct52)

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop_0': '', 'prop_5': {44: val5.to_dict()}})

        with self.assertRaises(TypeError):
            TestModel1.from_dict({
                'prop_0': '',
                'prop_5': [val5.to_dict(), None]
            })

    def test_handle_from_to_dict_for_open_generic_class(self):
        value61 = "value 6 1"
        struct61 = {'prop_0': '', 'prop_6': value61}
        obj61 = TestModel1.from_dict(struct61)
        self.assertEqual(obj61.prop_6, value61)
        self.assertEqual(obj61.to_dict(), struct61)

        value62 = ["value 6 2"]
        struct62 = {'prop_0': '', 'prop_6': value62}
        obj62 = TestModel1.from_dict(struct62)
        self.assertEqual(obj62.prop_6, value62)
        self.assertEqual(obj62.to_dict(), struct62)

        value63 = {"key 6 3": "value 6 3"}
        struct63 = {'prop_0': '', 'prop_6': value63}
        obj63 = TestModel1.from_dict(struct63)
        self.assertEqual(obj63.prop_6, value63)
        self.assertEqual(obj63.to_dict(), struct63)

        with self.assertRaises(TypeError):
            TestModel1.from_dict({'prop_0': '', 'prop_6': 64})

    def test_handle_comparisons(self):

        class A(ModelBase):

            def __init__(self, a, b):
                super().__init__(locals())

        self.assertEqual(A(1, 2), A(1, 2))
        self.assertNotEqual(A(1, 2), A(1, 3))

        class B(ModelBase):

            def __init__(self, a, b):
                super().__init__(locals())

        self.assertNotEqual(A(1, 2), B(1, 2))


if __name__ == '__main__':
    unittest.main()
