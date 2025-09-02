from math import inf
import sys
from metaflow.util import decompress_list, compress_list
import base64


def generate_input_paths(input_paths):
    # => run_id/step/:foo,bar
    # input_paths are base64 encoded due to Argo shenanigans
    decoded = base64.b64decode(input_paths).decode("utf-8")
    paths = decompress_list(decoded)

    # some of the paths are going to be malformed due to never having executed per conditional.
    # strip these out of the list.

    trimmed = [path for path in paths if not "{{" in path]
    return compress_list(trimmed, zlibmin=inf)


if __name__ == "__main__":
    print(generate_input_paths(sys.argv[1]))
