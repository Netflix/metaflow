from math import inf
import sys
from metaflow.util import decompress_list, compress_list
import base64


def generate_input_paths(input_paths, skippable_steps):
    # => run_id/step/:foo,bar
    # input_paths are base64 encoded due to Argo shenanigans
    decoded = base64.b64decode(input_paths).decode("utf-8")
    paths = decompress_list(decoded)

    # some of the paths are going to be malformed due to never having executed per conditional.
    # strip these out of the list.

    # all pathspecs of leading steps that executed.
    trimmed = [path for path in paths if not "{{" in path]

    # pathspecs of leading steps that are conditional, and should be used instead of non-conditional ones
    # e.g. the case of skipping switches: start -> case_step -> conditional_a or end
    conditionals = [
        path for path in trimmed if not any(step in path for step in skippable_steps)
    ]
    pathspecs_to_use = conditionals if conditionals else trimmed
    return compress_list(pathspecs_to_use, zlibmin=inf)


if __name__ == "__main__":
    input_paths = sys.argv[1]
    try:
        skippable_steps = sys.argv[2].split(",")
    except IndexError:
        skippable_steps = []

    print(generate_input_paths(input_paths, skippable_steps))
