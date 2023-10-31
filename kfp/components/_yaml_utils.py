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
import yaml
from collections import OrderedDict


def load_yaml(stream):
    #!!! Yaml should only be loaded using this function. Otherwise the dict ordering may be broken in Python versions prior to 3.6
    #See https://stackoverflow.com/questions/5121931/in-python-how-can-you-load-yaml-mappings-as-ordereddicts/21912744#21912744

    def ordered_load(stream,
                     Loader=yaml.SafeLoader,
                     object_pairs_hook=OrderedDict):

        class OrderedLoader(Loader):
            pass

        def construct_mapping(loader, node):
            loader.flatten_mapping(node)
            return object_pairs_hook(loader.construct_pairs(node))

        OrderedLoader.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping)
        return yaml.load(stream, OrderedLoader)

    return ordered_load(stream)


def dump_yaml(data):
    #See https://stackoverflow.com/questions/5121931/in-python-how-can-you-load-yaml-mappings-as-ordereddicts/21912744#21912744

    def ordered_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):

        class OrderedDumper(Dumper):
            pass

        def _dict_representer(dumper, data):
            return dumper.represent_mapping(
                yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())

        OrderedDumper.add_representer(OrderedDict, _dict_representer)
        OrderedDumper.add_representer(dict, _dict_representer)

        #Hack to force the code (multi-line string) to be output using the '|' style.
        def represent_str_or_text(self, data):
            style = None
            if data.find('\n') >= 0:  #Multiple lines
                #print('Switching style for multiline text:' + data)
                style = '|'
            if data.lower() in [
                    'y', 'n', 'yes', 'no', 'true', 'false', 'on', 'off'
            ]:
                style = '"'
            return self.represent_scalar(u'tag:yaml.org,2002:str', data, style)

        OrderedDumper.add_representer(str, represent_str_or_text)

        return yaml.dump(data, stream, OrderedDumper, **kwds)

    return ordered_dump(data, default_flow_style=None)
