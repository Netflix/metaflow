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

from kfp.dsl import artifact_utils
from typing import Any, List


class ComplexMetricsBase(object):

    def get_schema(self):
        """Returns the set YAML schema for the metric class.

        Returns:
          YAML schema of the metrics type.
        """
        return self._schema

    def get_metrics(self):
        """Returns the stored metrics.

        The metrics are type checked against the set schema.

        Returns:
          Dictionary of metrics data in the format of the set schema.
        """
        artifact_utils.verify_schema_instance(self._schema, self._values)
        return self._values

    def __init__(self, schema_file: str):
        self._schema = artifact_utils.read_schema_file(schema_file)

        self._type_name, self._metric_fields = artifact_utils.parse_schema(
            self._schema)

        self._values = {}


class ConfidenceMetrics(ComplexMetricsBase):
    """Metrics class representing a Confidence Metrics."""

    # Initialization flag to support setattr / getattr behavior.
    _initialized = False

    def __getattr__(self, name: str) -> Any:
        """Custom __getattr__ to allow access to metrics schema fields."""

        if name not in self._metric_fields:
            raise AttributeError('No field: {} in metrics.'.format(name))

        return self._values[name]

    def __setattr__(self, name: str, value: Any):
        """Custom __setattr__ to allow access to metrics schema fields."""

        if not self._initialized:
            object.__setattr__(self, name, value)
            return

        if name not in self._metric_fields:
            raise RuntimeError(
                'Field: {} not defined in metirc schema'.format(name))

        self._values[name] = value

    def __init__(self):
        super().__init__('confidence_metrics.yaml')
        self._initialized = True


class ConfusionMatrix(ComplexMetricsBase):
    """Metrics class representing a confusion matrix."""

    def __init__(self):
        super().__init__('confusion_matrix.yaml')

        self._matrix = [[]]
        self._categories = []
        self._initialized = True

    def set_categories(self, categories: List[str]):
        """Sets the categories for Confusion Matrix.

        Args:
          categories: List of strings specifying the categories.
        """
        self._categories = []
        annotation_specs = []
        for category in categories:
            annotation_spec = {'displayName': category}
            self._categories.append(category)
            annotation_specs.append(annotation_spec)

        self._values['annotationSpecs'] = annotation_specs
        self._matrix = [[0
                         for i in range(len(self._categories))]
                        for j in range(len(self._categories))]
        self._values['row'] = self._matrix

    def log_row(self, row_category: str, row: List[int]):
        """Logs a confusion matrix row.

        Args:
          row_category: Category to which the row belongs.
          row: List of integers specifying the values for the row.

        Raises:
          ValueError: If row_category is not in the list of categories set in
            set_categories or size of the row does not match the size of
            categories.
        """
        if row_category not in self._categories:
            raise ValueError('Invalid category: {} passed. Expected one of: {}'.\
              format(row_category, self._categories))

        if len(row) != len(self._categories):
            raise ValueError('Invalid row. Expected size: {} got: {}'.\
              format(len(self._categories), len(row)))

        self._matrix[self._categories.index(row_category)] = row

    def log_cell(self, row_category: str, col_category: str, value: int):
        """Logs a cell in the confusion matrix.

        Args:
          row_category: String representing the name of the row category.
          col_category: String representing the name of the column category.
          value: Int value of the cell.

        Raises:
          ValueError: If row_category or col_category is not in the list of
           categories set in set_categories.
        """
        if row_category not in self._categories:
            raise ValueError('Invalid category: {} passed. Expected one of: {}'.\
              format(row_category, self._categories))

        if col_category not in self._categories:
            raise ValueError('Invalid category: {} passed. Expected one of: {}'.\
              format(row_category, self._categories))

        self._matrix[self._categories.index(row_category)][
            self._categories.index(col_category)] = value

    def load_matrix(self, categories: List[str], matrix: List[List[int]]):
        """Supports bulk loading the whole confusion matrix.

        Args:
          categories: List of the category names.
          matrix: Complete confusion matrix.

        Raises:
          ValueError: Length of categories does not match number of rows or columns.
        """
        self.set_categories(categories)

        if len(matrix) != len(categories):
            raise ValueError('Invalid matrix: {} passed for categories: {}'.\
              format(matrix, categories))

        for index in range(len(categories)):
            if len(matrix[index]) != len(categories):
                raise ValueError('Invalid matrix: {} passed for categories: {}'.\
                  format(matrix, categories))

            self.log_row(categories[index], matrix[index])
