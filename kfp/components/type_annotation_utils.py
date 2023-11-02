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
"""Utilities for handling Python type annotation."""

import re
from typing import TypeVar, Union

T = TypeVar('T')


def maybe_strip_optional_from_annotation(annotation: T) -> T:
    """Strips 'Optional' from 'Optional[<type>]' if applicable.

    For example::
      Optional[str] -> str
      str -> str
      List[int] -> List[int]

    Args:
      annotation: The original type annotation which may or may not has
        `Optional`.

    Returns:
      The type inside Optional[] if Optional exists, otherwise the original type.
    """
    if getattr(annotation, '__origin__',
               None) is Union and annotation.__args__[1] is type(None):
        return annotation.__args__[0]
    return annotation


def get_short_type_name(type_name: str) -> str:
    """Extracts the short form type name.

    This method is used for looking up serializer for a given type.

    For example::
      typing.List -> List
      typing.List[int] -> List
      typing.Dict[str, str] -> Dict
      List -> List
      str -> str

    Args:
      type_name: The original type name.

    Returns:
      The short form type name or the original name if pattern doesn't match.
    """
    match = re.match('(typing\.)?(?P<type>\w+)(?:\[.+\])?', type_name)
    if match:
        return match.group('type')
    else:
        return type_name
