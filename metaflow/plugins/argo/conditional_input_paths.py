from math import inf
import sys
from metaflow.util import decompress_list, compress_list


def generate_input_paths(input_paths):
    # Note the non-default separator due to difficulties setting parameter values from conditional step outputs.
    paths = decompress_list(input_paths, separator="%")

    # some of the paths are going to be malformed due to never having executed per conditional.
    # strip these out of the list.

    trimmed = [path for path in paths if not path.endswith("/no-task")]
    return compress_list(trimmed, zlibmin=inf)


if __name__ == "__main__":
    print(generate_input_paths(sys.argv[1]))
