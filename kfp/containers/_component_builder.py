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

import os
import sys
import tempfile
import logging
import shutil
from collections import OrderedDict
from typing import Callable, Dict, List, Optional

from deprecated.sphinx import deprecated

from ..components._components import _create_task_factory_from_component_spec
from ..components._python_op import _func_to_component_spec
from ._container_builder import ContainerBuilder
from kfp import components
from kfp import dsl
from kfp.components import _components
from kfp.components import _structures
from kfp.containers import entrypoint

V2_COMPONENT_ANNOTATION = 'pipelines.kubeflow.org/component_v2'
_PROGRAM_LAUNCHER_CMD = 'program_path=$(mktemp)\nprintf "%s" "$0" > ' \
                        '"$program_path"\npython3 -u "$program_path" "$@"\n'


class VersionedDependency(object):
    """DependencyVersion specifies the versions."""

    def __init__(self, name, version=None, min_version=None, max_version=None):
        """if version is specified, no need for min_version or max_version; if
        both are specified, version is adopted."""
        self._name = name
        if version is not None:
            self._min_version = version
            self._max_version = version
        else:
            self._min_version = min_version
            self._max_version = max_version

    @property
    def name(self):
        return self._name

    @property
    def min_version(self):
        return self._min_version

    @min_version.setter
    def min_version(self, min_version):
        self._min_version = min_version

    def has_min_version(self):
        return self._min_version != None

    @property
    def max_version(self):
        return self._max_version

    @max_version.setter
    def max_version(self, max_version):
        self._max_version = max_version

    def has_max_version(self):
        return self._max_version != None

    def has_versions(self):
        return (self.has_min_version()) or (self.has_max_version())


class DependencyHelper(object):
    """DependencyHelper manages software dependency information."""

    def __init__(self):
        self._PYTHON_PACKAGE = 'PYTHON_PACKAGE'
        self._dependency = {self._PYTHON_PACKAGE: OrderedDict()}

    @property
    def python_packages(self):
        return self._dependency[self._PYTHON_PACKAGE]

    def add_python_package(self, dependency, override=True):
        """add_single_python_package adds a dependency for the python package.

        Args:
          name: package name
          version: it could be a specific version(1.10.0), or a range(>=1.0,<=2.0)
            if not specified, the default is resolved automatically by the pip system.
          override: whether to override the version if already existing in the dependency.
        """
        if dependency.name in self.python_packages and not override:
            return
        self.python_packages[dependency.name] = dependency

    def generate_pip_requirements(self, target_file):
        """write the python packages to a requirement file the generated file
        follows the order of which the packages are added."""
        with open(target_file, 'w') as f:
            for name, version in self.python_packages.items():
                version = self.python_packages[name]
                version_str = ''
                if version.has_min_version():
                    version_str += ' >= ' + version.min_version + ','
                if version.has_max_version():
                    version_str += ' <= ' + version.max_version + ','
                f.write(name + version_str.rstrip(',') + '\n')


def _dependency_to_requirements(dependency=[], filename='requirements.txt'):
    """
    Generates a requirement file based on the dependency
    Args:
      dependency (list): a list of VersionedDependency, which includes the package name and versions
      filename (str): requirement file name, default as requirements.txt
  """
    dependency_helper = DependencyHelper()
    for version in dependency:
        dependency_helper.add_python_package(version)
    dependency_helper.generate_pip_requirements(filename)


def _generate_dockerfile(filename: str,
                         base_image: str,
                         requirement_filename: Optional[str] = None,
                         add_files: Optional[Dict[str, str]] = None):
    """
    generates dockerfiles
    Args:
      filename (str): target file name for the dockerfile.
      base_image (str): the base image name.
      requirement_filename (str): requirement file name
      add_files (Dict[str, str]): Map containing the files thats should be added to the container. add_files maps the build context relative source paths to the container destination paths.
  """
    with open(filename, 'w') as f:
        f.write('FROM ' + base_image + '\n')
        f.write(
            'RUN apt-get update -y && apt-get install --no-install-recommends -y -q python3 python3-pip python3-setuptools\n'
        )
        if requirement_filename is not None:
            f.write('ADD ' + requirement_filename + ' /ml/requirements.txt\n')
            f.write('RUN python3 -m pip install -r /ml/requirements.txt\n')

        for src_path, dst_path in (add_files or {}).items():
            f.write('ADD ' + src_path + ' ' + dst_path + '\n')


def _configure_logger(logger):
    """_configure_logger configures the logger such that the info level logs go
    to the stdout and the error(or above) level logs go to the stderr.

    It is important for the Jupyter notebook log rendering
    """
    if hasattr(_configure_logger, 'configured'):
        # Skip the logger configuration the second time this function
        # is called to avoid multiple streamhandlers bound to the logger.
        return
    setattr(_configure_logger, 'configured', 'true')
    logger.setLevel(logging.INFO)
    info_handler = logging.StreamHandler(stream=sys.stdout)
    info_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    info_handler.setFormatter(
        logging.Formatter(
            '%(asctime)s:%(levelname)s:%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'))
    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.addFilter(lambda record: record.levelno > logging.INFO)
    error_handler.setFormatter(
        logging.Formatter(
            '%(asctime)s:%(levelname)s:%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(info_handler)
    logger.addHandler(error_handler)


def _purge_program_launching_code(
        commands: List[str],
        entrypoint_container_path: Optional[str] = None,
        is_v2: bool = False) -> str:
    """Replaces the inline Python code with calling a local program.

    For example,
    Before: sh -ec '... && python3 -u ...' 'import sys ...' --param1 ...
    After:  python -u /ml/main.py --param1 ...

    Args:
      commands: The container commands to be replaced.
      entrypoint_container_path: The path to the entrypoint program in the
        container.
      is_v2: Whether the component being generated is a v2 component. Default is
        False.

    Returns:
      The originally generated inline Python code.
    """
    if not (is_v2 or entrypoint_container_path):
        raise ValueError(
            'Only v2 component has default entrypoint path. '
            'Conventional KFP component needs to specify container '
            'entrypoint explicitly. For example, /ml/main.py')
    program_launcher_index = commands.index(_PROGRAM_LAUNCHER_CMD)
    # When there're preinstallation package specified when converting to component
    # spec the index will be 3, otherwise it'll be 2.
    assert program_launcher_index in [2, 3]
    program_code_index = program_launcher_index + 1
    result = commands[program_code_index]
    if is_v2:
        # TODO: Implement the v2 component entrypoint on KFP.
        # The following are just placeholders.
        commands[program_code_index] = 'kfp.containers.entrypoint'
        commands.pop(program_launcher_index)
        commands[program_launcher_index - 1] = '-m'
        commands[program_launcher_index - 2] = 'python'
    else:
        commands[program_code_index] = entrypoint_container_path
        commands.pop(program_launcher_index)
        commands[program_launcher_index - 1] = '-u'  # -ec => -u
        # sh => python3 or python2
        commands[program_launcher_index - 2] = 'python'

    return result


def build_python_component(
        component_func: Callable,
        target_image: str,
        base_image: Optional[str] = None,
        dependency: Optional[List[VersionedDependency]] = None,
        staging_gcs_path: Optional[str] = None,
        timeout: int = 600,
        namespace: Optional[str] = None,
        target_component_file: Optional[str] = None,
        is_v2: bool = False):
    """build_component automatically builds a container image for the
    component_func based on the base_image and pushes to the target_image.

    Args:
      component_func (python function): The python function to build components
        upon.
      base_image (str): Docker image to use as a base image.
      target_image (str): Full URI to push the target image.
      staging_gcs_path (str): GCS blob that can store temporary build files.
      target_image (str): The target image path.
      timeout (int): The timeout for the image build(in secs), default is 600
        seconds.
      namespace (str): The namespace within which to run the kubernetes Kaniko
        job. If the job is running on GKE and value is None the underlying
        functions will use the default namespace from GKE.
      dependency (list): The list of VersionedDependency, which includes the
        package name and versions, default is empty.
      target_component_file (str): The path to save the generated component YAML
        spec.
      is_v2: Whether or not generating a v2 KFP component, default
        is false.

    Raises:
      ValueError: The function is not decorated with python_component decorator or
        the python_version is neither python2 nor python3
    """

    _configure_logger(logging.getLogger())

    if component_func is None:
        raise ValueError('component_func must not be None')
    if target_image is None:
        raise ValueError('target_image must not be None')

    if staging_gcs_path is None:
        raise ValueError('staging_gcs_path must not be None')

    if base_image is None:
        base_image = getattr(component_func, '_component_base_image', None)
    if base_image is None:
        from ..components._python_op import default_base_image_or_builder
        base_image = default_base_image_or_builder
        if isinstance(base_image, Callable):
            base_image = base_image()
    if not dependency:
        dependency = []

    logging.info('Build an image that is based on ' + base_image +
                 ' and push the image to ' + target_image)

    component_spec = _func_to_component_spec(
        component_func, base_image=base_image)

    if is_v2:
        # TODO: Remove this warning once we make v2 component compatible with KFP
        # v1 stack.
        logging.warning(
            'Currently V2 component is only compatible with v2 KFP.')
        # Annotate the component to be a V2 one.
        if not component_spec.metadata:
            component_spec.metadata = _structures.MetadataSpec()
        if not component_spec.metadata.annotations:
            component_spec.metadata.annotations = {}
        component_spec.metadata.annotations[V2_COMPONENT_ANNOTATION] = 'true'

    command_line_args = component_spec.implementation.container.command

    # The relative path to put the Python program code.
    program_path = 'ml/main.py'
    # The relative path used when building a V2 component.
    v2_entrypoint_path = None
    # Python program code extracted from the component spec.
    program_code = None

    if is_v2:

        program_code = _purge_program_launching_code(
            commands=command_line_args, is_v2=True)

        # Override user program args for new-styled component.
        # TODO: The actual program args will be changed after we support v2
        # component on KFP.
        # For v2 component, the received command line args are fixed as follows:
        # --executor_input_str
        # {Executor input pb message at runtime}
        # --function_name
        # {The name of user defined function}
        # --output_metadata_path
        # {The place to write output metadata JSON file}
        program_args = [
            '--executor_input_str',
            _structures.ExecutorInputPlaceholder(),
            '--{}'.format(entrypoint.FN_NAME_ARG), component_func.__name__,
            '--output_metadata_path',
            _structures.OutputMetadataPlaceholder()
        ]

        component_spec.implementation.container.args = program_args
    else:
        program_code = _purge_program_launching_code(
            commands=command_line_args,
            entrypoint_container_path='/' + program_path)

    arc_docker_filename = 'Dockerfile'
    arc_requirement_filename = 'requirements.txt'

    with tempfile.TemporaryDirectory() as local_build_dir:
        # Write the program code to a file in the context directory
        local_python_filepath = os.path.join(local_build_dir, program_path)
        os.makedirs(os.path.dirname(local_python_filepath), exist_ok=True)

        with open(local_python_filepath, 'w') as f:
            f.write(program_code)

        # Generate the python package requirements file in the context directory
        local_requirement_filepath = os.path.join(local_build_dir,
                                                  arc_requirement_filename)
        if is_v2:
            # For v2 components, KFP are expected to be packed in the container.
            dependency.append(
                VersionedDependency(name='kfp', min_version='1.4.0'))

        _dependency_to_requirements(dependency, local_requirement_filepath)

        # Generate Dockerfile in the context directory
        local_docker_filepath = os.path.join(local_build_dir,
                                             arc_docker_filename)
        add_files = {program_path: '/' + program_path}

        _generate_dockerfile(
            local_docker_filepath,
            base_image,
            arc_requirement_filename,
            add_files=add_files)

        logging.info('Building and pushing container image.')
        container_builder = ContainerBuilder(staging_gcs_path, target_image,
                                             namespace)
        image_name_with_digest = container_builder.build(
            local_build_dir, arc_docker_filename, target_image, timeout)

    component_spec.implementation.container.image = image_name_with_digest

    # Optionally writing the component definition to a local file for sharing
    target_component_file = target_component_file or getattr(
        component_func, '_component_target_component_file', None)
    if target_component_file:
        component_spec.save(target_component_file)

    task_factory_function = _create_task_factory_from_component_spec(
        component_spec)
    return task_factory_function


@deprecated(
    version='0.1.32',
    reason='`build_docker_image` is deprecated. Use `kfp.containers.build_image_from_working_dir` instead.'
)
def build_docker_image(staging_gcs_path,
                       target_image,
                       dockerfile_path,
                       timeout=600,
                       namespace=None):
    """build_docker_image automatically builds a container image based on the
    specification in the dockerfile and pushes to the target_image.

    Args:
      staging_gcs_path (str): GCS blob that can store temporary build files
      target_image (str): gcr path to push the final image
      dockerfile_path (str): local path to the dockerfile
      timeout (int): the timeout for the image build(in secs), default is 600 seconds
      namespace (str): the namespace within which to run the kubernetes kaniko job. Default is None. If the
      job is running on GKE and value is None the underlying functions will use the default namespace from GKE.
    """
    _configure_logger(logging.getLogger())

    with tempfile.TemporaryDirectory() as local_build_dir:
        dockerfile_rel_path = 'Dockerfile'
        dst_dockerfile_path = os.path.join(local_build_dir, dockerfile_rel_path)
        shutil.copyfile(dockerfile_path, dst_dockerfile_path)

        container_builder = ContainerBuilder(
            staging_gcs_path, target_image, namespace=namespace)
        image_name_with_digest = container_builder.build(
            local_build_dir, dockerfile_rel_path, target_image, timeout)

    logging.info('Build image complete.')
    return image_name_with_digest
