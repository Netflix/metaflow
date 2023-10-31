# Lint as: python3
# Copyright 2019 The Kubeflow Authors. All Rights Reserved.
#
# Licensed under the Apache License,Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Supporting tools and classes for diagnose_me."""

import json
import subprocess
from typing import List, Text


class ExecutorResponse(object):
    """Class for keeping track of output of _executor methods.

    Data model for executing commands and capturing their response. This class
    defines the data model layer for execution results, based on MVC design
    pattern.

    TODO() This class should be extended to contain data structure to better
    represent the underlying data instaed of dict for various response types.
    """

    def execute_command(self, command_list: List[Text]):
        """Executes the command in command_list.

        sets values for _stdout,_std_err, and returncode accordingly.

        TODO(): This method is kept in ExecutorResponse for simplicity, however this
        deviates from MVP design pattern. It should be factored out in future.

        Args:
          command_list: A List of strings that represts the command and parameters
            to be executed.

        Returns:
          Instance of utility.ExecutorResponse.
        """

        try:
            # TODO() switch to process.run to simplify the code.
            process = subprocess.Popen(
                command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            self._stdout = stdout.decode('utf-8')
            self._stderr = stderr.decode('utf-8')
            self._returncode = process.returncode
        except OSError as e:
            self._stderr = e
            self._stdout = ''
            self._returncode = e.errno
        self._parse_raw_input()
        return self

    def _parse_raw_input(self):
        """Parses the raw input and popluates _json and _parsed properies."""
        try:
            self._parsed_output = json.loads(self._stdout)
            self._json = self._stdout
        except json.JSONDecodeError:
            self._json = json.dumps(self._stdout)
            self._parsed_output = self._stdout

    @property
    def parsed_output(self) -> Text:
        """Json load results of stdout or raw results if stdout was not
        Json."""
        return self._parsed_output

    @property
    def has_error(self) -> bool:
        """Returns true if execution error code was not 0."""
        return self._returncode != 0

    @property
    def json_output(self) -> Text:
        """Run results in stdout in json format."""
        return self._parsed_output

    @property
    def stderr(self):
        return self._stderr
