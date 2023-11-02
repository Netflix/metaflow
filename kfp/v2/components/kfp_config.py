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
from typing import Dict, Optional

import configparser
import pathlib
import warnings

_KFP_CONFIG_FILE = 'kfp_config.ini'

_COMPONENTS_SECTION = 'Components'


class KFPConfig():
    """Class for managing KFP component configuration.

    The configuration is .ini file named `kfp_config.ini` that can be parsed by
    Python's native configparser module. Currently, this class supports a single
    `Components` section, which lists components as key-value pairs. The key is
    the component name (i.e. the function name), and the value is the path to
    the file containing this function. The path is usually relative from the
    location of the configuration file, but absolute paths should also work.

    At runtime, the KFP v2 Executor, defined in executor_main.py, will look
    for this configuration file in its current working directory. If found,
    it will load its contents, and use this to find the file containing the
    component to execute.

    Example of the file's contents:

    [Components]
    my_component_1 = my_dir_1/my_component_1.py
    my_component_2 = my_dir_2/my_component_2.py
    ...
    """

    def __init__(self, config_directory: Optional[pathlib.Path] = None):
        """Creates a KFPConfig object.

        Loads the config from an existing `kfp_config.ini` file if found.

        Args:
            config_directory: Looks for a file named `kfp_config.ini` in this
                directory. Defaults to the current directory.
        """
        self._config_parser = configparser.ConfigParser()
        # Preserve case for keys.
        self._config_parser.optionxform = lambda x: x

        if config_directory is None:
            self._config_filepath = pathlib.Path(_KFP_CONFIG_FILE)
        else:
            self._config_filepath = config_directory / _KFP_CONFIG_FILE

        try:
            with open(str(self._config_filepath), 'r') as f:
                self._config_parser.read_file(f)
        except IOError:
            warnings.warn('No existing KFP Config file found')

        if not self._config_parser.has_section(_COMPONENTS_SECTION):
            self._config_parser.add_section(_COMPONENTS_SECTION)

        self._components = {}

    def add_component(self, function_name: str, path: pathlib.Path):
        """Adds a KFP component.

        Args:
            function_name: The name of the component function.
            path: A path to the file containing the component.
        """
        self._components[function_name] = str(path)

    def save(self):
        """Writes out a KFP config file."""
        # Always write out components in alphabetical order for determinism,
        # especially in tests.
        for function_name in sorted(self._components.keys()):
            self._config_parser[_COMPONENTS_SECTION][
                function_name] = self._components[function_name]

        with open(str(self._config_filepath), 'w') as f:
            self._config_parser.write(f)

    def get_components(self) -> Dict[str, pathlib.Path]:
        """Returns a list of known KFP components.

        Returns:
            A dictionary from component name (function name) to a pathlib.Path
            pointing to the Python file with this component's definition.
        """
        result = {
            function_name: pathlib.Path(module_path) for function_name,
            module_path in self._config_parser[_COMPONENTS_SECTION].items()
        }
        return result
