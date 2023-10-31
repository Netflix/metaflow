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

import functools
from typing import Callable, Optional, List

from kfp.v2.components import component_factory


def component(func: Optional[Callable] = None,
              *,
              base_image: Optional[str] = None,
              target_image: Optional[str] = None,
              packages_to_install: List[str] = None,
              output_component_file: Optional[str] = None,
              install_kfp_package: bool = True,
              kfp_package_path: Optional[str] = None):
    """Decorator for Python-function based components in KFP v2.

    A KFP v2 component can either be a lightweight component, or a containerized
    one.

    If target_image is not specified, this function creates a lightweight
    component. A lightweight component is a self-contained Python function that
    includes all necessary imports and dependencies. In lightweight components,
    packages_to_install will be used to install dependencies at runtime. The
    parameters install_kfp_package and kfp_package_path can be used to control
    how KFP should be installed when the lightweight component is executed.

    If target_image is specified, this function creates a component definition
    based around the target_image. The assumption is that the function in func
    will be packaged by KFP into this target_image. Use the KFP CLI's `build`
    command to package func into target_image.

    Example usage:

    from kfp.v2 import dsl
    @dsl.component
    def my_function_one(input: str, output: Output[Model]):
      ...

    @dsl.component(
      base_image='python:3.9',
      output_component_file='my_function.yaml'
    )
    def my_function_two(input: Input[Mode])):
      ...

    @dsl.pipeline(pipeline_root='...',
                  name='my-pipeline')
    def pipeline():
      my_function_one_task = my_function_one(input=...)
      my_function_two_task = my_function_two(input=my_function_one_task.outputs..

    Args:
        func: The python function to create a component from. The function
            should have type annotations for all its arguments, indicating how
            it is intended to be used (e.g. as an input/output Artifact object,
            a plain parameter, or a path to a file).
        base_image: The image to use when executing func. It should
            contain a default Python interpreter that is compatible with KFP.
        packages_to_install: A list of optional packages to install before
            executing func. These will always be installed at component runtime.
        output_component_file: If specified, this function will write a
            shareable/loadable version of the component spec into this file.
        install_kfp_package: Specifies if we should add a KFP Python package to
            packages_to_install. Lightweight Python functions always require
            an installation of KFP in base_image to work. If you specify
            a base_image that already contains KFP, you can set this to False.
            This flag is ignored when target_image is specified, which implies
            we're building a containerized component. Containerized components
            will always install KFP as part of the build process.
        kfp_package_path: Specifies the location from which to install KFP. By
            default, this will try to install from PyPi using the same version
            as that used when this component was created. KFP developers can
            choose to override this to point to a Github pull request or
            other pip-compatible location when testing changes to lightweight
            Python functions.

    Returns:
        A component task factory that can be used in pipeline definitions.
  """
    if func is None:
        return functools.partial(
            component,
            base_image=base_image,
            target_image=target_image,
            packages_to_install=packages_to_install,
            output_component_file=output_component_file,
            install_kfp_package=install_kfp_package,
            kfp_package_path=kfp_package_path)

    return component_factory.create_component_from_func(
        func,
        base_image=base_image,
        target_image=target_image,
        packages_to_install=packages_to_install,
        output_component_file=output_component_file,
        install_kfp_package=install_kfp_package,
        kfp_package_path=kfp_package_path)
