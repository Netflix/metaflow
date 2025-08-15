from math import inf
import sys
from metaflow.util import decompress_list, compress_list


def generate_input_paths(input_paths):
    # => run_id/step/:foo,bar
    paths = decompress_list(input_paths)

    # some of the paths are going to be malformed due to never having executed per conditional.
    # strip these out of the list.

    trimmed = [path for path in paths if not "{{" in path]
    return compress_list(trimmed, zlibmin=inf)


if __name__ == "__main__":
    print(generate_input_paths(sys.argv[1]))
