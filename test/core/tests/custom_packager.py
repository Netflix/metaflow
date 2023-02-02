from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class CustomPackagerTest(MetaflowTest):
  """
  Test that a user provided packager works.
  """

  PRIORITY = 3

  HEADER = """
import os, sys
from metaflow import packager

with open('input.txt', mode='w') as f:
  f.write("Regular Text File")

def my_package_generator():
  yield ("input.txt", "target.txt")

@packager(generator=my_package_generator)
"""

  @steps(1, ["all"])
  def step_all(self):
    pass

  def check_results(self, flow, checker):
    checker.assert_package_file_exists("test_flow.py")
    checker.assert_package_file_content("target.txt", b"Regular Text File")
