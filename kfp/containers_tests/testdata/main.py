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
"""User module under test."""
from typing import NamedTuple
from kfp import components
from kfp.dsl import artifact
from kfp.dsl import ontology_artifacts


def test_func(
    test_param: str, test_artifact: components.InputArtifact('Dataset'),
    test_output1: components.OutputArtifact('Model')
) -> NamedTuple('Outputs', [('test_output2', str)]):
    assert test_param == 'hello from producer'
    # In the associated test case, input artifact is produced by conventional
    # KFP components, thus no concrete artifact type can be determined.
    assert isinstance(test_artifact, artifact.Artifact)
    assert isinstance(test_output1, ontology_artifacts.Model)
    assert test_output1.uri
    from collections import namedtuple

    Outputs = namedtuple('Outputs', 'test_output2')
    return Outputs('bye world')


def test_func2(
    test_param: str, test_artifact: components.InputArtifact('Dataset'),
    test_output1: components.OutputArtifact('Model')
) -> NamedTuple('Outputs', [('test_output2', str)]):
    assert test_param == 'hello from producer'
    # In the associated test case, input artifact is produced by a new-styled
    # KFP components with metadata, thus it's expected to be deserialized to
    # Dataset object.
    assert isinstance(test_artifact, ontology_artifacts.Dataset)
    assert isinstance(test_output1, ontology_artifacts.Model)
    assert test_output1.uri
    from collections import namedtuple

    Outputs = namedtuple('Outputs', 'test_output2')
    return Outputs('bye world')
