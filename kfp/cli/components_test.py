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
"""Tests for `components` command group in KFP CLI."""
import contextlib
import importlib
import pathlib
import sys
import textwrap
from typing import List, Optional, Union
import unittest
from unittest import mock

from typer import testing

# Docker is an optional install, but we need the import to succeed for tests.
# So we patch it before importing kfp.cli.components.
if importlib.util.find_spec('docker') is None:
    sys.modules['docker'] = mock.Mock()
from kfp.cli import components

_COMPONENT_TEMPLATE = '''
from kfp.v2.dsl import *

@component(
  base_image={base_image},
  target_image={target_image},
  output_component_file={output_component_file})
def {func_name}():
    pass
'''


def _make_component(func_name: str,
                    base_image: Optional[str] = None,
                    target_image: Optional[str] = None,
                    output_component_file: Optional[str] = None) -> str:
    return textwrap.dedent('''
    from kfp.v2.dsl import *

    @component(
        base_image={base_image},
        target_image={target_image},
        output_component_file={output_component_file})
    def {func_name}():
        pass
    ''').format(
        base_image=repr(base_image),
        target_image=repr(target_image),
        output_component_file=repr(output_component_file),
        func_name=func_name)


def _write_file(filename: str, file_contents: str):
    filepath = pathlib.Path(filename)
    filepath.parent.mkdir(exist_ok=True, parents=True)
    filepath.write_text(file_contents)


def _write_components(filename: str, component_template: Union[List[str], str]):
    if isinstance(component_template, list):
        file_contents = '\n\n'.join(component_template)
    else:
        file_contents = component_template
    _write_file(filename=filename, file_contents=file_contents)


class Test(unittest.TestCase):

    def setUp(self) -> None:
        self._runner = testing.CliRunner()
        components._DOCKER_IS_PRESENT = True

        patcher = mock.patch('docker.from_env')
        self._docker_client = patcher.start().return_value
        self._docker_client.images.build.return_value = [{
            'stream': 'Build logs'
        }]
        self._docker_client.images.push.return_value = [{'status': 'Pushed'}]
        self.addCleanup(patcher.stop)

        self._app = components.app
        with contextlib.ExitStack() as stack:
            stack.enter_context(self._runner.isolated_filesystem())
            self._working_dir = pathlib.Path.cwd()
            self.addCleanup(stack.pop_all().close)

        return super().setUp()

    def assertFileExists(self, path: str):
        path_under_test_dir = self._working_dir / path
        self.assertTrue(path_under_test_dir, f'File {path} does not exist!')

    def assertFileExistsAndContains(self, path: str, expected_content: str):
        self.assertFileExists(path)
        path_under_test_dir = self._working_dir / path
        got_content = path_under_test_dir.read_text()
        self.assertEqual(got_content, expected_content)

    def testKFPConfigForSingleFile(self):
        preprocess_component = _make_component(
            func_name='preprocess', target_image='custom-image')
        train_component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py',
                          [preprocess_component, train_component])

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 0)

        self.assertFileExistsAndContains(
            'kfp_config.ini',
            textwrap.dedent('''\
            [Components]
            preprocess = components.py
            train = components.py

            '''))

    def testKFPConfigForSingleFileUnderNestedDirectory(self):
        preprocess_component = _make_component(
            func_name='preprocess', target_image='custom-image')
        train_component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('dir1/dir2/dir3/components.py',
                          [preprocess_component, train_component])

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 0)

        self.assertFileExistsAndContains(
            'kfp_config.ini',
            textwrap.dedent('''\
            [Components]
            preprocess = dir1/dir2/dir3/components.py
            train = dir1/dir2/dir3/components.py

            '''))

    def testKFPConfigForMultipleFiles(self):
        component = _make_component(
            func_name='preprocess', target_image='custom-image')
        _write_components('preprocess_component.py', component)

        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('train_component.py', component)

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 0)

        self.assertFileExistsAndContains(
            'kfp_config.ini',
            textwrap.dedent('''\
            [Components]
            preprocess = preprocess_component.py
            train = train_component.py

            '''))

    def testKFPConfigForMultipleFilesUnderNestedDirectories(self):
        component = _make_component(
            func_name='preprocess', target_image='custom-image')
        _write_components('preprocess/preprocess_component.py', component)

        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('train/train_component.py', component)

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 0)

        self.assertFileExistsAndContains(
            'kfp_config.ini',
            textwrap.dedent('''\
            [Components]
            preprocess = preprocess/preprocess_component.py
            train = train/train_component.py

            '''))

    def testTargetImageMustBeTheSameInAllComponents(self):
        component_one = _make_component(func_name='one', target_image='image-1')
        component_two = _make_component(func_name='two', target_image='image-1')
        _write_components('one_two/one_two.py', [component_one, component_two])

        component_three = _make_component(
            func_name='three', target_image='image-2')
        component_four = _make_component(
            func_name='four', target_image='image-3')
        _write_components('three_four/three_four.py',
                          [component_three, component_four])

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 1)

    def testTargetImageMustBeTheSameInAllComponents(self):
        component_one = _make_component(
            func_name='one', base_image='image-1', target_image='target-image')
        component_two = _make_component(
            func_name='two', base_image='image-1', target_image='target-image')
        _write_components('one_two/one_two.py', [component_one, component_two])

        component_three = _make_component(
            func_name='three',
            base_image='image-2',
            target_image='target-image')
        component_four = _make_component(
            func_name='four', base_image='image-3', target_image='target-image')
        _write_components('three_four/three_four.py',
                          [component_three, component_four])

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 1)

    def testComponentFilepatternCanBeUsedToRestrictDiscovery(self):
        component = _make_component(
            func_name='preprocess', target_image='custom-image')
        _write_components('preprocess/preprocess_component.py', component)

        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('train/train_component.py', component)

        result = self._runner.invoke(
            self._app,
            [
                'build',
                str(self._working_dir), '--component-filepattern=train/*'
            ],
        )
        self.assertEqual(result.exit_code, 0)

        self.assertFileExistsAndContains(
            'kfp_config.ini',
            textwrap.dedent('''\
            [Components]
            train = train/train_component.py

            '''))

    def testEmptyRequirementsTxtFileIsGenerated(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        result = self._runner.invoke(self._app,
                                     ['build', str(self._working_dir)])
        self.assertEqual(result.exit_code, 0)
        self.assertFileExistsAndContains('requirements.txt',
                                         '# Generated by KFP.\n')

    def testExistingRequirementsTxtFileIsUnchanged(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        _write_file('requirements.txt', 'Some pre-existing content')

        result = self._runner.invoke(self._app,
                                     ['build', str(self._working_dir)])
        self.assertEqual(result.exit_code, 0)
        self.assertFileExistsAndContains('requirements.txt',
                                         'Some pre-existing content')

    def testDockerignoreFileIsGenerated(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        result = self._runner.invoke(self._app,
                                     ['build', str(self._working_dir)])
        self.assertEqual(result.exit_code, 0)
        self.assertFileExistsAndContains(
            '.dockerignore',
            textwrap.dedent('''\
            # Generated by KFP.

            component_metadata/
            '''))

    def testExistingDockerignoreFileIsUnchanged(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        _write_file('.dockerignore', 'Some pre-existing content')

        result = self._runner.invoke(self._app,
                                     ['build', str(self._working_dir)])
        self.assertEqual(result.exit_code, 0)
        self.assertFileExistsAndContains('.dockerignore',
                                         'Some pre-existing content')

    def testDockerEngineIsSupported(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir), '--engine=docker'])
        self.assertEqual(result.exit_code, 0)
        self._docker_client.api.build.assert_called_once()
        self._docker_client.images.push.assert_called_once_with(
            'custom-image', stream=True, decode=True)

    def testKanikoEngineIsNotSupported(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)
        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir), '--engine=kaniko'],
        )
        self.assertEqual(result.exit_code, 1)
        self._docker_client.api.build.assert_not_called()
        self._docker_client.images.push.assert_not_called()

    def testCloudBuildEngineIsNotSupported(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)
        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir), '--engine=cloudbuild'],
        )
        self.assertEqual(result.exit_code, 1)
        self._docker_client.api.build.assert_not_called()
        self._docker_client.images.push.assert_not_called()

    def testDockerClientIsCalledToBuildAndPushByDefault(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 0)

        self._docker_client.api.build.assert_called_once()
        self._docker_client.images.push.assert_called_once_with(
            'custom-image', stream=True, decode=True)

    def testDockerClientIsCalledToBuildButSkipsPushing(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir), '--no-push-image'],
        )
        self.assertEqual(result.exit_code, 0)

        self._docker_client.api.build.assert_called_once()
        self._docker_client.images.push.assert_not_called()

    @mock.patch('kfp.__version__', '1.2.3')
    def testDockerfileIsCreatedCorrectly(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 0)
        self._docker_client.api.build.assert_called_once()
        self.assertFileExistsAndContains(
            'Dockerfile',
            textwrap.dedent('''\
                # Generated by KFP.

                FROM python:3.7

                WORKDIR /usr/local/src/kfp/components
                COPY requirements.txt requirements.txt
                RUN pip install --no-cache-dir -r requirements.txt

                RUN pip install --no-cache-dir kfp==1.2.3
                COPY . .
                '''))

    def testExistingDockerfileIsUnchangedByDefault(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)
        _write_file('Dockerfile', 'Existing Dockerfile contents')

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir)],
        )
        self.assertEqual(result.exit_code, 0)
        self._docker_client.api.build.assert_called_once()
        self.assertFileExistsAndContains('Dockerfile',
                                         'Existing Dockerfile contents')

    @mock.patch('kfp.__version__', '1.2.3')
    def testExistingDockerfileCanBeOverwritten(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)
        _write_file('Dockerfile', 'Existing Dockerfile contents')

        result = self._runner.invoke(
            self._app,
            ['build', str(self._working_dir), '--overwrite-dockerfile'],
        )
        self.assertEqual(result.exit_code, 0)
        self._docker_client.api.build.assert_called_once()
        self.assertFileExistsAndContains(
            'Dockerfile',
            textwrap.dedent('''\
                # Generated by KFP.

                FROM python:3.7

                WORKDIR /usr/local/src/kfp/components
                COPY requirements.txt requirements.txt
                RUN pip install --no-cache-dir -r requirements.txt

                RUN pip install --no-cache-dir kfp==1.2.3
                COPY . .
                '''))

    def testDockerfileCanContainCustomKFPPackage(self):
        component = _make_component(
            func_name='train', target_image='custom-image')
        _write_components('components.py', component)

        result = self._runner.invoke(
            self._app,
            [
                'build',
                str(self._working_dir),
                '--kfp-package-path=/Some/localdir/containing/kfp/source'
            ],
        )
        self.assertEqual(result.exit_code, 0)
        self._docker_client.api.build.assert_called_once()
        self.assertFileExistsAndContains(
            'Dockerfile',
            textwrap.dedent('''\
                # Generated by KFP.

                FROM python:3.7

                WORKDIR /usr/local/src/kfp/components
                COPY requirements.txt requirements.txt
                RUN pip install --no-cache-dir -r requirements.txt

                RUN pip install --no-cache-dir /Some/localdir/containing/kfp/source
                COPY . .
                '''))


if __name__ == '__main__':
    unittest.main()
