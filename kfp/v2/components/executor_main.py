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
import argparse
import json
import logging
import os
import sys

from kfp.v2.components import executor as component_executor
from kfp.v2.components import kfp_config
from kfp.v2.components import utils


def _setup_logging():
    logging_format = '[KFP Executor %(asctime)s %(levelname)s]: %(message)s'
    logging.basicConfig(
        stream=sys.stdout, format=logging_format, level=logging.INFO)


def executor_main():
    _setup_logging()
    parser = argparse.ArgumentParser(description='KFP Component Executor.')

    parser.add_argument(
        '--component_module_path',
        type=str,
        help='Path to a module containing the KFP component.')

    parser.add_argument(
        '--function_to_execute',
        type=str,
        required=True,
        help='The name of the component function in '
        '--component_module_path file that is to be executed.')

    parser.add_argument(
        '--executor_input',
        type=str,
        help='JSON-serialized ExecutorInput from the orchestrator. '
        'This should contain inputs and placeholders for outputs.')

    args, _ = parser.parse_known_args()

    func_name = args.function_to_execute
    module_path = None
    module_directory = None
    module_name = None

    if args.component_module_path is not None:
        logging.info(
            'Looking for component `{}` in --component_module_path `{}`'.format(
                func_name, args.component_module_path))
        module_path = args.component_module_path
        module_directory = os.path.dirname(args.component_module_path)
        module_name = os.path.basename(args.component_module_path)[:-len('.py')]
    else:
        # Look for module directory using kfp_config.ini
        logging.info('--component_module_path is not specified. Looking for'
                     ' component `{}` in config file `kfp_config.ini`'
                     ' instead'.format(func_name))
        config = kfp_config.KFPConfig()
        components = config.get_components()
        if not components:
            raise RuntimeError('No components found in `kfp_config.ini`')
        try:
            module_path = components[func_name]
        except KeyError:
            raise RuntimeError(
                'Could not find component `{}` in `kfp_config.ini`. Found the '
                ' following components instead:\n{}'.format(
                    func_name, components))

        module_directory = str(module_path.parent)
        module_name = str(module_path.name)[:-len('.py')]

    logging.info(
        'Loading KFP component "{}" from {} (directory "{}" and module name'
        ' "{}")'.format(func_name, module_path, module_directory, module_name))

    module = utils.load_module(
        module_name=module_name, module_directory=module_directory)

    executor_input = json.loads(args.executor_input)
    function_to_execute = getattr(module, func_name)

    executor = component_executor.Executor(
        executor_input=executor_input, function_to_execute=function_to_execute)

    executor.execute()


if __name__ == '__main__':
    executor_main()
