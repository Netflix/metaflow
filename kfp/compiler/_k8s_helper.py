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

from kfp import dsl


def sanitize_k8s_name(name, allow_capital_underscore=False):
    """From _make_kubernetes_name sanitize_k8s_name cleans and converts the
    names in the workflow.

    Args:
      name: original name,
      allow_capital_underscore: whether to allow capital letter and underscore
        in this name.

    Returns:
      sanitized name.
    """
    if allow_capital_underscore:
        return re.sub('-+', '-', re.sub('[^-_0-9A-Za-z]+', '-',
                                        name)).lstrip('-').rstrip('-')
    else:
        return re.sub('-+', '-', re.sub('[^-0-9a-z]+', '-',
                                        name.lower())).lstrip('-').rstrip('-')


def convert_k8s_obj_to_json(k8s_obj):
    """Builds a JSON K8s object.

    If obj is None, return None.
    If obj is str, int, long, float, bool, return directly.
    If obj is datetime.datetime, datetime.date
        convert to string in iso8601 format.
    If obj is list, sanitize each element in the list.
    If obj is dict, return the dict.
    If obj is swagger model, return the properties dict.

    Args:
      obj: The data to serialize.
    Returns: The serialized form of data.
    """

    from six import text_type, integer_types, iteritems
    PRIMITIVE_TYPES = (float, bool, bytes, text_type) + integer_types
    from datetime import date, datetime
    if k8s_obj is None:
        return None
    elif isinstance(k8s_obj, PRIMITIVE_TYPES):
        return k8s_obj
    elif isinstance(k8s_obj, list):
        return [convert_k8s_obj_to_json(sub_obj) for sub_obj in k8s_obj]
    elif isinstance(k8s_obj, tuple):
        return tuple(convert_k8s_obj_to_json(sub_obj) for sub_obj in k8s_obj)
    elif isinstance(k8s_obj, (datetime, date)):
        return k8s_obj.isoformat()
    elif isinstance(k8s_obj, dsl.PipelineParam):
        if isinstance(k8s_obj.value, str):
            return k8s_obj.value
        return '{{inputs.parameters.%s}}' % k8s_obj.full_name

    if isinstance(k8s_obj, dict):
        obj_dict = k8s_obj
    else:
        # Convert model obj to dict except
        # attributes `swagger_types`, `attribute_map`
        # and attributes which value is not None.
        # Convert attribute name to json key in
        # model definition for request.
        obj_dict = {
            k8s_obj.attribute_map[attr]: getattr(k8s_obj, attr)
            for attr in k8s_obj.attribute_map
            if getattr(k8s_obj, attr) is not None
        }

    return {
        key: convert_k8s_obj_to_json(val) for key, val in iteritems(obj_dict)
    }
