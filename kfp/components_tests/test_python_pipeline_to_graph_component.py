# Copyright 2019 The Kubeflow Authors
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
from collections import OrderedDict
from pathlib import Path

from .. import components as comp
from ..components._python_to_graph_component import create_graph_component_spec_from_pipeline_func


class PythonPipelineToGraphComponentTestCase(unittest.TestCase):

    def test_handle_creating_graph_component_from_pipeline_that_uses_container_components(
            self):
        test_data_dir = Path(__file__).parent / 'test_data'
        producer_op = comp.load_component_from_file(
            str(test_data_dir /
                'component_with_0_inputs_and_2_outputs.component.yaml'))
        processor_op = comp.load_component_from_file(
            str(test_data_dir /
                'component_with_2_inputs_and_2_outputs.component.yaml'))
        consumer_op = comp.load_component_from_file(
            str(test_data_dir /
                'component_with_2_inputs_and_0_outputs.component.yaml'))

        def pipeline1(pipeline_param_1: int):
            producer_task = producer_op()
            processor_task = processor_op(pipeline_param_1,
                                          producer_task.outputs['Output 2'])
            consumer_task = consumer_op(processor_task.outputs['Output 1'],
                                        processor_task.outputs['Output 2'])

            return OrderedDict(
                [  # You can safely return normal dict in python 3.6+
                    ('Pipeline output 1', producer_task.outputs['Output 1']),
                    ('Pipeline output 2', processor_task.outputs['Output 2']),
                ])

        graph_component = create_graph_component_spec_from_pipeline_func(
            pipeline1)

        self.assertEqual(len(graph_component.inputs), 1)
        self.assertListEqual(
            [input.name for input in graph_component.inputs],
            ['pipeline_param_1'
            ])  #Relies on human name conversion function stability
        self.assertListEqual(
            [output.name for output in graph_component.outputs],
            ['Pipeline output 1', 'Pipeline output 2'])
        self.assertEqual(len(graph_component.implementation.graph.tasks), 3)

    def test_create_component_from_real_pipeline_retail_product_stockout_prediction(
            self):
        from .test_data.retail_product_stockout_prediction_pipeline import retail_product_stockout_prediction_pipeline

        graph_component = create_graph_component_spec_from_pipeline_func(
            retail_product_stockout_prediction_pipeline)

        import yaml
        expected_component_spec_path = str(
            Path(__file__).parent / 'test_data' /
            'retail_product_stockout_prediction_pipeline.component.yaml')
        with open(expected_component_spec_path) as f:
            expected_dict = yaml.safe_load(f)

        self.assertEqual(expected_dict, graph_component.to_dict())


if __name__ == '__main__':
    unittest.main()
