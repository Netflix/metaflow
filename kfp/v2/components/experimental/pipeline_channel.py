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
"""Definition of PipelineChannel."""
import abc
import dataclasses
import json
import re
from typing import Dict, List, Optional, Union

from kfp.v2.components import utils
from kfp.v2.components.types.experimental import type_utils


@dataclasses.dataclass
class ConditionOperator:
    """Represents a condition expression to be used in dsl.Condition().

    Attributes:
      operator: The operator of the condition.
      left_operand: The left operand.
      right_operand: The right operand.
    """
    operator: str
    left_operand: Union['PipelineParameterChannel', type_utils.PARAMETER_TYPES]
    right_operand: Union['PipelineParameterChannel', type_utils.PARAMETER_TYPES]


# The string template used to generate the placeholder of a PipelineChannel.
_PIPELINE_CHANNEL_PLACEHOLDER_TEMPLATE = (
    '{{channel:task=%s;name=%s;type=%s;}}')
# The regex for parsing PipelineChannel placeholders from a string.
_PIPELINE_CHANNEL_PLACEHOLDER_REGEX = (
    r'{{channel:task=([\w\s_-]*);name=([\w\s_-]+);type=([\w\s{}":_-]*);}}')


class PipelineChannel(abc.ABC):
    """Represents a future value that is passed between pipeline components.

    A PipelineChannel object can be used as a pipeline function argument so that
    it will be a pipeline artifact or parameter that shows up in ML Pipelines
    system UI. It can also represent an intermediate value passed between
    components.

    Attributes:
        name: The name of the pipeline channel.
        channel_type: The type of the pipeline channel.
        task_name: The name of the task that produces the pipeline channel.
            None means it is not produced by any task, so if None, either user
            constructs it directly (for providing an immediate value), or it is
            a pipeline function argument.
        pattern: The serialized string regex pattern this pipeline channel
            created from.
    """

    @abc.abstractmethod
    def __init__(
        self,
        name: str,
        channel_type: Union[str, Dict],
        task_name: Optional[str] = None,
    ):
        """Initializes a PipelineChannel instance.

        Args:
            name: The name of the pipeline channel. The name will be sanitized
                to be k8s compatible.
            channel_type: The type of the pipeline channel.
            task_name: Optional; The name of the task that produces the pipeline
                channel. If provided, the task name will be sanitized to be k8s
                compatible.

        Raises:
            ValueError: If name or task_name contains invalid characters.
            ValueError: If both task_name and value are set.
        """
        valid_name_regex = r'^[A-Za-z][A-Za-z0-9\s_-]*$'
        if not re.match(valid_name_regex, name):
            raise ValueError(
                'Only letters, numbers, spaces, "_", and "-" are allowed in the '
                'name. Must begin with a letter. Got name: {}'.format(name))

        self.name = name
        self.channel_type = channel_type
        # ensure value is None even if empty string or empty list/dict
        # so that serialization and unserialization remain consistent
        # (i.e. None => '' => None)
        self.task_name = task_name or None

    @property
    def full_name(self) -> str:
        """Unique name for the PipelineChannel."""
        return f'{self.task_name}-{self.name}' if self.task_name else self.name

    @property
    def pattern(self) -> str:
        """Unique pattern for the PipelineChannel."""
        return str(self)

    def __str__(self) -> str:
        """String representation of the PipelineChannel.

        The string representation is a string identifier so we can mix
        the PipelineChannel inline with other strings such as arguments.
        For example, we can support: ['echo %s' % param] as the
        container command and later a compiler can replace the
        placeholder '{{pipeline_channel:task=%s;name=%s;type=%s}}' with
        its own parameter identifier.
        """
        task_name = self.task_name or ''
        name = self.name
        channel_type = self.channel_type or ''
        if isinstance(channel_type, dict):
            channel_type = json.dumps(channel_type)
        return _PIPELINE_CHANNEL_PLACEHOLDER_TEMPLATE % (task_name, name,
                                                         channel_type)

    def __repr__(self) -> str:
        """Representation of the PipelineChannel.

        We make repr return the placeholder string so that if someone
        uses str()-based serialization of complex objects containing
        `PipelineChannel`, it works properly. (e.g. str([1, 2, 3,
        kfp.v2.dsl.PipelineParameterChannel("aaa"), 4, 5, 6,]))
        """
        return str(self)

    def __hash__(self) -> int:
        """Returns the hash of a PipelineChannel."""
        return hash(self.pattern)

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


class PipelineParameterChannel(PipelineChannel):
    """Represents a pipeline parameter channel.

    Attributes:
      name: The name of the pipeline channel.
      channel_type: The type of the pipeline channel.
      task_name: The name of the task that produces the pipeline channel.
        None means it is not produced by any task, so if None, either user
        constructs it directly (for providing an immediate value), or it is a
        pipeline function argument.
      pattern: The serialized string regex pattern this pipeline channel created
        from.
      value: The actual value of the pipeline channel. If provided, the
        pipeline channel is "resolved" immediately.
    """

    def __init__(
        self,
        name: str,
        channel_type: Union[str, Dict],
        task_name: Optional[str] = None,
        value: Optional[type_utils.PARAMETER_TYPES] = None,
    ):
        """Initializes a PipelineArtifactChannel instance.

        Args:
          name: The name of the pipeline channel.
          channel_type: The type of the pipeline channel.
          task_name: Optional; The name of the task that produces the pipeline
            channel.
          value: Optional; The actual value of the pipeline channel.

        Raises:
          ValueError: If name or task_name contains invalid characters.
          ValueError: If both task_name and value are set.
          TypeError: If the channel type is not a parameter type.
        """
        if task_name and value:
            raise ValueError('task_name and value cannot be both set.')

        if not type_utils.is_parameter_type(channel_type):
            raise TypeError(f'{channel_type} is not a parameter type.')

        self.value = value

        super(PipelineParameterChannel, self).__init__(
            name=name,
            channel_type=channel_type,
            task_name=task_name,
        )


class PipelineArtifactChannel(PipelineChannel):
    """Represents a pipeline artifact channel.

    Attributes:
      name: The name of the pipeline channel.
      channel_type: The type of the pipeline channel.
      task_name: The name of the task that produces the pipeline channel.
        A pipeline artifact channel is always produced by some task.
      pattern: The serialized string regex pattern this pipeline channel created
        from.
    """

    def __init__(
        self,
        name: str,
        channel_type: Union[str, Dict],
        task_name: str,
    ):
        """Initializes a PipelineArtifactChannel instance.

        Args:
          name: The name of the pipeline channel.
          channel_type: The type of the pipeline channel.
          task_name: The name of the task that produces the pipeline channel.

        Raises:
          ValueError: If name or task_name contains invalid characters.
          TypeError: If the channel type is not an artifact type.
        """
        if type_utils.is_parameter_type(channel_type):
            raise TypeError(f'{channel_type} is not an artifact type.')

        super(PipelineArtifactChannel, self).__init__(
            name=name,
            channel_type=channel_type,
            task_name=task_name,
        )


def create_pipeline_channel(
    name: str,
    channel_type: Union[str, Dict],
    task_name: Optional[str],
    value: Optional[type_utils.PARAMETER_TYPES] = None,
) -> PipelineChannel:
    """Creates a PipelineChannel object.

    Args:
        name: The name of the channel.
        channel_type: The type of the channel, which decides whether it is an
            PipelineParameterChannel or PipelineArtifactChannel
        task_name: Optional; the task that produced the channel.
        value: Optional; the realized value for a channel.

    Returns:
        A PipelineParameterChannel or PipelineArtifactChannel object.
    """
    if type_utils.is_parameter_type(channel_type):
        return PipelineParameterChannel(
            name=name,
            channel_type=channel_type,
            task_name=task_name,
            value=value,
        )
    else:
        return PipelineArtifactChannel(
            name=name,
            channel_type=channel_type,
            task_name=task_name,
        )


def extract_pipeline_channels_from_string(
        payload: str) -> List[PipelineChannel]:
    """Extracts a list of PipelineChannel instances from the payload string.

    Note: this function removes all duplicate matches.

    Args:
      payload: A string that may contain serialized PipelineChannels.

    Returns:
      A list of PipelineChannels found from the payload.
    """
    matches = re.findall(_PIPELINE_CHANNEL_PLACEHOLDER_REGEX, payload)
    unique_channels = set()
    for match in matches:
        task_name, name, channel_type = match

        # channel_type could be either a string (e.g. "Integer") or a dictionary
        # (e.g.: {"custom_type": {"custom_property": "some_value"}}).
        # Try loading it into dictionary, if failed, it means channel_type is a
        # string.
        try:
            channel_type = json.loads(channel_type)
        except json.JSONDecodeError:
            pass

        if type_utils.is_parameter_type(channel_type):
            pipeline_channel = PipelineParameterChannel(
                name=name,
                channel_type=channel_type,
                task_name=task_name,
            )
        else:
            pipeline_channel = PipelineArtifactChannel(
                name=name,
                channel_type=channel_type,
                task_name=task_name,
            )
        unique_channels.add(pipeline_channel)

    return list(unique_channels)


def extract_pipeline_channels_from_any(
    payload: Union[PipelineChannel, str, list, tuple, dict]
) -> List[PipelineChannel]:
    """Recursively extract PipelineChannels from any object or list of objects.

    Args:
      payload: An object that contains serialized PipelineChannels or k8
        definition objects.

    Returns:
      A list of PipelineChannels found from the payload.
    """
    if not payload:
        return []

    if isinstance(payload, PipelineChannel):
        return [payload]

    if isinstance(payload, str):
        return list(set(extract_pipeline_channels_from_string(payload)))

    if isinstance(payload, list) or isinstance(payload, tuple):
        pipeline_channels = []
        for item in payload:
            pipeline_channels += extract_pipeline_channels_from_any(item)
        return list(set(pipeline_channels))

    if isinstance(payload, dict):
        pipeline_channels = []
        for key, value in payload.items():
            pipeline_channels += extract_pipeline_channels_from_any(key)
            pipeline_channels += extract_pipeline_channels_from_any(value)
        return list(set(pipeline_channels))

    # TODO(chensun): extract PipelineChannel from v2 container spec?

    return []
