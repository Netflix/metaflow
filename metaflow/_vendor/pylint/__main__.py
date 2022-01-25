#!/usr/bin/env python

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

from metaflow._vendor import pylint

pylint.modify_sys_path()
pylint.run_pylint()
