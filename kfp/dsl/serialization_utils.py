# Copyright 2020 The Kubeflow Authors
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
"""Utilities for serialization/deserialization."""
from typing import Any, Dict
import yaml


# Serialize None as blank instead of 'null'.
def _represent_none(self, _):
    return self.represent_scalar('tag:yaml.org,2002:null', '')


class _NoneAsBlankDumper(yaml.SafeDumper):
    """Alternative dumper to print YAML literal.

    The behavior is mostly identical to yaml.SafeDumper, except for this
    dumper dumps None object as blank, instead of 'null'.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_representer(type(None), _represent_none)


def yaml_dump(data: Dict[str, Any]) -> str:
    """Dumps YAML string.

    None will be represented as blank.
    """
    return yaml.dump(data, Dumper=_NoneAsBlankDumper)
