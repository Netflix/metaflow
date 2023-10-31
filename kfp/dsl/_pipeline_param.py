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
import re
from collections import namedtuple
from typing import Dict, List, Optional, Union

# TODO: Move this to a separate class
# For now, this identifies a condition with only "==" operator supported.
ConditionOperator = namedtuple('ConditionOperator',
                               'operator operand1 operand2')
PipelineParamTuple = namedtuple('PipelineParamTuple', 'name op pattern')


def sanitize_k8s_name(name, allow_capital_underscore=False):
    """Cleans and converts the names in the workflow.

    Args:
      name: original name,
      allow_capital_underscore: whether to allow capital letter and underscore in
        this name.

    Returns:
      A sanitized name.
    """
    if allow_capital_underscore:
        return re.sub('-+', '-', re.sub('[^-_0-9A-Za-z]+', '-',
                                        name)).lstrip('-').rstrip('-')
    else:
        return re.sub('-+', '-', re.sub('[^-0-9a-z]+', '-',
                                        name.lower())).lstrip('-').rstrip('-')


def match_serialized_pipelineparam(payload: str) -> List[PipelineParamTuple]:
    """Matches the supplied serialized pipelineparam.

    Args:
      payloads: The search space for the serialized pipelineparams.

    Returns:
      The matched pipeline params we found in the supplied payload.
    """
    matches = re.findall(r'{{pipelineparam:op=([\w\s_-]*);name=([\w\s_-]+)}}',
                         payload)
    param_tuples = []
    for match in matches:
        pattern = '{{pipelineparam:op=%s;name=%s}}' % (match[0], match[1])
        param_tuples.append(
            PipelineParamTuple(
                name=sanitize_k8s_name(match[1], True),
                op=sanitize_k8s_name(match[0]),
                pattern=pattern))
    return param_tuples


def _extract_pipelineparams(
        payloads: Union[str, List[str]]) -> List['PipelineParam']:
    """Extracts a list of PipelineParam instances from the payload string.

    Note: this function removes all duplicate matches.

    Args:
      payload: a string/a list of strings that contains serialized pipelineparams

    Return: List[]
    """
    if isinstance(payloads, str):
        payloads = [payloads]
    param_tuples = []
    for payload in payloads:
        param_tuples += match_serialized_pipelineparam(payload)
    pipeline_params = []
    for param_tuple in list(set(param_tuples)):
        pipeline_params.append(
            PipelineParam(
                param_tuple.name, param_tuple.op, pattern=param_tuple.pattern))
    return pipeline_params


def extract_pipelineparams_from_any(
    payload: Union['PipelineParam', str, list, tuple, dict]
) -> List['PipelineParam']:
    """Recursively extract PipelineParam instances or serialized string from
    any object or list of objects.

    Args:
      payload (str or k8_obj or list[str or k8_obj]): a string/a list of strings
        that contains serialized pipelineparams or a k8 definition object.

    Return: List[PipelineParam]
    """
    if not payload:
        return []

    # PipelineParam
    if isinstance(payload, PipelineParam):
        return [payload]

    # str
    if isinstance(payload, str):
        return list(set(_extract_pipelineparams(payload)))

    # list or tuple
    if isinstance(payload, list) or isinstance(payload, tuple):
        pipeline_params = []
        for item in payload:
            pipeline_params += extract_pipelineparams_from_any(item)
        return list(set(pipeline_params))

    # dict
    if isinstance(payload, dict):
        pipeline_params = []
        for key, value in payload.items():
            pipeline_params += extract_pipelineparams_from_any(key)
            pipeline_params += extract_pipelineparams_from_any(value)
        return list(set(pipeline_params))

    # k8s OpenAPI object
    if hasattr(payload, 'attribute_map') and isinstance(payload.attribute_map,
                                                        dict):
        pipeline_params = []
        for key in payload.attribute_map:
            pipeline_params += extract_pipelineparams_from_any(
                getattr(payload, key))

        return list(set(pipeline_params))

    # return empty list
    return []


class PipelineParam(object):
    """Representing a future value that is passed between pipeline components.

    A PipelineParam object can be used as a pipeline function argument so that it
    will be a pipeline parameter that shows up in ML Pipelines system UI. It can
    also represent an intermediate value passed between components.

    Args:
      name: name of the pipeline parameter.
      op_name: the name of the operation that produces the PipelineParam. None
        means it is not produced by any operator, so if None, either user
        constructs it directly (for providing an immediate value), or it is a
        pipeline function argument.
      value: The actual value of the PipelineParam. If provided, the PipelineParam
        is "resolved" immediately. For now, we support string only.
      param_type: the type of the PipelineParam.
      pattern: the serialized string regex pattern this pipeline parameter created
        from.

    Raises: ValueError in name or op_name contains invalid characters, or both
      op_name and value are set.
    """

    def __init__(self,
                 name: str,
                 op_name: Optional[str] = None,
                 value: Optional[str] = None,
                 param_type: Optional[Union[str, Dict]] = None,
                 pattern: Optional[str] = None):
        valid_name_regex = r'^[A-Za-z][A-Za-z0-9\s_-]*$'
        if not re.match(valid_name_regex, name):
            raise ValueError(
                'Only letters, numbers, spaces, "_", and "-" are allowed in name. '
                'Must begin with a letter. Got name: {}'.format(name))

        if op_name and value:
            raise ValueError('op_name and value cannot be both set.')

        self.name = name
        # ensure value is None even if empty string or empty list
        # so that serialization and unserialization remain consistent
        # (i.e. None => '' => None)
        self.op_name = op_name if op_name else None
        self.value = value
        self.param_type = param_type
        self.pattern = pattern or str(self)

    @property
    def full_name(self):
        """Unique name in the argo yaml for the PipelineParam."""
        if self.op_name:
            return self.op_name + '-' + self.name
        return self.name

    def __str__(self):
        """String representation.

        The string representation is a string identifier so we can mix
        the PipelineParam inline with other strings such as arguments.
        For example, we can support: ['echo %s' % param] as the
        container command and later a compiler can replace the
        placeholder "{{pipelineparam:op=%s;name=%s}}" with its own
        parameter identifier.
        """

        #This is deleted because if users specify default values to PipelineParam,
        # The compiler can not detect it as the value is not NULL.
        #if self.value:
        #  return str(self.value)

        op_name = self.op_name if self.op_name else ''
        return '{{pipelineparam:op=%s;name=%s}}' % (op_name, self.name)

    def __repr__(self):
        # return str({self.__class__.__name__: self.__dict__})
        # We make repr return the placeholder string so that if someone uses
        # str()-based serialization of complex objects containing `PipelineParam`,
        # it works properly.
        # (e.g. str([1, 2, 3, kfp.dsl.PipelineParam("aaa"), 4, 5, 6,]))
        return str(self)

    def to_struct(self):
        # Used by the json serializer. Outputs a JSON-serializable representation of
        # the object
        return str(self)

    def __eq__(self, other):
        return ConditionOperator('==', self, other)

    def __ne__(self, other):
        return ConditionOperator('!=', self, other)

    def __lt__(self, other):
        return ConditionOperator('<', self, other)

    def __le__(self, other):
        return ConditionOperator('<=', self, other)

    def __gt__(self, other):
        return ConditionOperator('>', self, other)

    def __ge__(self, other):
        return ConditionOperator('>=', self, other)

    def __hash__(self):
        return hash((self.op_name, self.name))

    def ignore_type(self):
        """ignore_type ignores the type information such that type checking
        would also pass."""
        self.param_type = None
        return self
