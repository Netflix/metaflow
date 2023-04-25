from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class CustomPackagerTest(MetaflowTest):
    """
    Test that a user provided packager works.
    """

    PRIORITY = 3

    HEADER = """
import os, sys
from pathlib import Path

from metaflow import packager

with open('input.txt', mode='w') as f:
  f.write("Input Text")

Path('subdir').mkdir(parents=True, exist_ok=True)
with open('subdir/library.py', mode='w') as f:
  f.write("CONST = 42")
with open('subdir/readme.txt', mode='w') as f:
  f.write("Regular Text File")

def my_package_generator():
  yield ("input.txt", "target.txt")

@packager(content={ "input.txt": "target.txt", "subdir": "package/something" })
"""

    @steps(1, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        checker.assert_package_file_existence("test_flow.py")
        checker.assert_package_file_content("target.txt", b"Input Text")
        checker.assert_package_file_content(
          "package/something/library.py", b"CONST = 42"
        )
        checker.assert_package_file_existence(
          "package/something/readme.txt", exists=False
        )
