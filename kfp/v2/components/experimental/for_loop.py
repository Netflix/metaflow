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
"""Classes and methods that supports argument for ParallelFor."""

import re
from typing import Any, Dict, List, Optional, Tuple, Union, get_type_hints

from kfp.v2.components.experimental import pipeline_channel

ItemList = List[Union[int, float, str, Dict[str, Any]]]


def _get_loop_item_type(type_name: str) -> Optional[str]:
    """Extracts the loop item type.

    This method is used for extract the item type from a collection type.
    For example:

        List[str] -> str
        typing.List[int] -> int
        typing.Sequence[str] -> str
        List -> None
        str -> None

    Args:
        type_name: The collection type name, like `List`, Sequence`, etc.

    Returns:
        The collection item type or None if no match found.
    """
    match = re.match('(typing\.)?(?:\w+)(?:\[(?P<item_type>.+)\])', type_name)
    if match:
        return match.group('item_type').lstrip().rstrip()
    else:
        return None


def _get_subvar_type(type_name: str) -> Optional[str]:
    """Extracts the subvar type.

    This method is used for extract the value type from a dictionary type.
    For example:

        Dict[str, int] -> int
        typing.Mapping[str, float] -> float

    Args:
        type_name: The dictionary type.

    Returns:
        The dictionary value type or None if no match found.
    """
    match = re.match(
        '(typing\.)?(?:\w+)(?:\[\s*(?:\w+)\s*,\s*(?P<value_type>.+)\])',
        type_name)
    if match:
        return match.group('value_type').lstrip().rstrip()
    else:
        return None


class LoopArgument(pipeline_channel.PipelineChannel):
    """Represents the argument that are looped over in a ParallelFor loop.

    The class shouldn't be instantiated by the end user, rather it is
    created automatically by a ParallelFor ops group.

    To create a LoopArgument instance, use one of its factory methods::

        LoopArgument.from_pipeline_channel(...)
        LoopArgument.from_raw_items(...)


    Attributes:
        items_or_pipeline_channel: The raw items or the PipelineChannel object
            this LoopArgument is associated to.
    """
    LOOP_ITEM_NAME_BASE = 'loop-item'
    LOOP_ITEM_PARAM_NAME_BASE = 'loop-item-param'

    def __init__(
        self,
        items: Union[ItemList, pipeline_channel.PipelineChannel],
        name_code: Optional[str] = None,
        name_override: Optional[str] = None,
        **kwargs,
    ):
        """Initializes a LoopArguments object.

        Args:
            items: List of items to loop over.  If a list of dicts then, all
                dicts must have the same keys and every key must be a legal
                Python variable name.
            name_code: A unique code used to identify these loop arguments.
                Should match the code for the ParallelFor ops_group which created
                these LoopArguments. This prevents parameter name collisions.
            name_override: The override name for PipelineChannel.
            **kwargs: Any other keyword arguments passed down to PipelineChannel.
        """
        if (name_code is None) == (name_override is None):
            raise ValueError(
                'Expect one and only one of `name_code` and `name_override` to '
                'be specified.')

        if name_override is None:
            super().__init__(name=self._make_name(name_code), **kwargs)
        else:
            super().__init__(name=name_override, **kwargs)

        if not isinstance(items,
                          (list, tuple, pipeline_channel.PipelineChannel)):
            raise TypeError(
                f'Expected list, tuple, or PipelineChannel, got {items}.')

        if isinstance(items, tuple):
            items = list(items)

        self.items_or_pipeline_channel = items
        self.is_with_items_loop_argument = not isinstance(
            items, pipeline_channel.PipelineChannel)
        self._referenced_subvars: Dict[str, LoopArgumentVariable] = {}

        if isinstance(items, list) and isinstance(items[0], dict):
            subvar_names = set(items[0].keys())
            # then this block creates loop_arg.variable_a and loop_arg.variable_b
            for subvar_name in subvar_names:
                loop_arg_var = LoopArgumentVariable(
                    loop_argument=self,
                    subvar_name=subvar_name,
                )
                self._referenced_subvars[subvar_name] = loop_arg_var
                setattr(self, subvar_name, loop_arg_var)

    def __getattr__(self, name: str):
        # this is being overridden so that we can access subvariables of the
        # LoopArgument (i.e.: item.a) without knowing the subvariable names ahead
        # of time.

        return self._referenced_subvars.setdefault(
            name, LoopArgumentVariable(
                loop_argument=self,
                subvar_name=name,
            ))

    def _make_name(self, code: str):
        """Makes a name for this loop argument from a unique code."""
        return '{}-{}'.format(self.LOOP_ITEM_PARAM_NAME_BASE, code)

    @classmethod
    def from_pipeline_channel(
        cls,
        channel: pipeline_channel.PipelineChannel,
    ) -> 'LoopArgument':
        """Creates a LoopArgument object from a PipelineChannel object."""
        return LoopArgument(
            items=channel,
            name_override=channel.name + '-' + cls.LOOP_ITEM_NAME_BASE,
            task_name=channel.task_name,
            channel_type=_get_loop_item_type(channel.channel_type) or 'String',
        )

    @classmethod
    def from_raw_items(
        cls,
        raw_items: ItemList,
        name_code: str,
    ) -> 'LoopArgument':
        """Creates a LoopArgument object from raw item list."""
        if len(raw_items) == 0:
            raise ValueError('Got an empty item list for loop argument.')

        return LoopArgument(
            items=raw_items,
            name_code=name_code,
            channel_type=type(raw_items[0]).__name__,
        )

    @classmethod
    def name_is_loop_argument(cls, name: str) -> bool:
        """Returns True if the given channel name looks like a loop argument.

        Either it came from a withItems loop item or withParams loop
        item.
        """
        return  ('-' + cls.LOOP_ITEM_NAME_BASE) in name \
          or (cls.LOOP_ITEM_PARAM_NAME_BASE + '-') in name


class LoopArgumentVariable(pipeline_channel.PipelineChannel):
    """Represents a subvariable for a loop argument.

    This is used for cases where we're looping over maps, each of which contains
    several variables. If the user ran:

        with dsl.ParallelFor([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]) as item:
            ...

    Then there's one LoopArgumentVariable for 'a' and another for 'b'.

    Attributes:
        loop_argument: The original LoopArgument object this subvariable is
          attached to.
        subvar_name: The subvariable name.
    """
    SUBVAR_NAME_DELIMITER = '-subvar-'
    LEGAL_SUBVAR_NAME_REGEX = re.compile(r'^[a-zA-Z_][0-9a-zA-Z_]*$')

    def __init__(
        self,
        loop_argument: LoopArgument,
        subvar_name: str,
    ):
        """Initializes a LoopArgumentVariable instance.

        Args:
            loop_argument: The LoopArgument object this subvariable is based on
                a subvariable to.
            subvar_name: The name of this subvariable, which is the name of the
                dict key that spawned this subvariable.

        Raises:
            ValueError is subvar name is illegal.
        """
        if not self._subvar_name_is_legal(subvar_name):
            raise ValueError(
                f'Tried to create subvariable named {subvar_name}, but that is '
                'not a legal Python variable name.')

        self.subvar_name = subvar_name
        self.loop_argument = loop_argument

        super().__init__(
            name=self._get_name_override(
                loop_arg_name=loop_argument.name,
                subvar_name=subvar_name,
            ),
            task_name=loop_argument.task_name,
            channel_type=_get_subvar_type(loop_argument.channel_type) or
            'String',
        )

    @property
    def items_or_pipeline_channel(
            self) -> Union[ItemList, pipeline_channel.PipelineChannel]:
        """Returns the loop argument items."""
        return self.loop_argument.items_or_pipeline_chanenl

    @property
    def is_with_items_loop_argument(self) -> bool:
        """Whether the loop argument is originated from raw items."""
        return self.loop_argument.is_with_items_loop_argument

    def _subvar_name_is_legal(self, proposed_variable_name: str) -> bool:
        """Returns True if the subvar name is legal."""
        return re.match(self.LEGAL_SUBVAR_NAME_REGEX,
                        proposed_variable_name) is not None

    def _get_name_override(self, loop_arg_name: str, subvar_name: str) -> str:
        """Gets the name.

        Args:
            loop_arg_name: the name of the loop argument parameter that this
              LoopArgumentVariable is attached to.
            subvar_name: The name of this subvariable.

        Returns:
            The name of this loop arg variable.
        """
        return f'{loop_arg_name}{self.SUBVAR_NAME_DELIMITER}{subvar_name}'
