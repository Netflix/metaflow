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

import argparse
from typing import Optional
from kfp import dsl
import kfp.compiler
import os
import sys
from deprecated.sphinx import deprecated

_KF_PIPELINES_COMPILER_MODE_ENV = 'KF_PIPELINES_COMPILER_MODE'


def parse_arguments():
    """Parse command line arguments."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--py', type=str, help='local absolute path to a py file.')
    parser.add_argument(
        '--function',
        type=str,
        help='The name of the function to compile if there are multiple.')
    parser.add_argument(
        '--namespace', type=str, help='The namespace for the pipeline function')
    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='local path to the output workflow yaml file.')
    parser.add_argument(
        '--disable-type-check',
        action='store_true',
        help='disable the type check, default is enabled.')
    parser.add_argument(
        '--mode',
        type=str,
        help='compiler mode, defaults to V1, can also be V2_COMPATIBLE. You can override the default using env var KF_PIPELINES_COMPILER_MODE.'
    )

    args = parser.parse_args()
    return args


def _compile_pipeline_function(pipeline_funcs, function_name, output_path,
                               type_check,
                               mode: Optional[dsl.PipelineExecutionMode]):
    if len(pipeline_funcs) == 0:
        raise ValueError(
            'A function with @dsl.pipeline decorator is required in the py file.'
        )

    if len(pipeline_funcs) > 1 and not function_name:
        func_names = [x.__name__ for x in pipeline_funcs]
        raise ValueError(
            'There are multiple pipelines: %s. Please specify --function.' %
            func_names)

    if function_name:
        pipeline_func = next(
            (x for x in pipeline_funcs if x.__name__ == function_name), None)
        if not pipeline_func:
            raise ValueError('The function "%s" does not exist. '
                             'Did you forget @dsl.pipeline decoration?' %
                             function_name)
    else:
        pipeline_func = pipeline_funcs[0]

    kfp.compiler.Compiler(mode=mode).compile(pipeline_func, output_path,
                                             type_check)


class PipelineCollectorContext():

    def __enter__(self):
        pipeline_funcs = []

        def add_pipeline(func):
            pipeline_funcs.append(func)
            return func

        self.old_handler = dsl._pipeline._pipeline_decorator_handler
        dsl._pipeline._pipeline_decorator_handler = add_pipeline
        return pipeline_funcs

    def __exit__(self, *args):
        dsl._pipeline._pipeline_decorator_handler = self.old_handler


def compile_pyfile(pyfile, function_name, output_path, type_check,
                   mode: Optional[dsl.PipelineExecutionMode]):
    sys.path.insert(0, os.path.dirname(pyfile))
    try:
        filename = os.path.basename(pyfile)
        with PipelineCollectorContext() as pipeline_funcs:
            __import__(os.path.splitext(filename)[0])
        _compile_pipeline_function(pipeline_funcs, function_name, output_path,
                                   type_check, mode)
    finally:
        del sys.path[0]


def main():
    args = parse_arguments()
    if args.py is None:
        raise ValueError('The --py option must be specified.')
    mode_str = args.mode
    if not mode_str:
        mode_str = os.environ.get(_KF_PIPELINES_COMPILER_MODE_ENV, 'V1')
    mode = None
    if mode_str == 'V1_LEGACY' or mode_str == 'V1':
        mode = kfp.dsl.PipelineExecutionMode.V1_LEGACY
    elif mode_str == 'V2_COMPATIBLE':
        mode = kfp.dsl.PipelineExecutionMode.V2_COMPATIBLE
    elif mode_str == 'V2_ENGINE':
        mode = kfp.dsl.PipelineExecutionMode.V2_ENGINE
    else:
        raise ValueError(
            f'Got unexpected --mode option "{mode_str}", it must be one of V1, V2_COMPATIBLE or V2_ENGINE'
        )
    compile_pyfile(
        args.py,
        args.function,
        args.output,
        not args.disable_type_check,
        mode,
    )
