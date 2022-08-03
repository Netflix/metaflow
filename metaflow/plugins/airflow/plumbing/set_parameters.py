import os
import json
import sys


def export_parameters(output_file):
    input = json.loads(os.environ.get("METAFLOW_PARAMETERS", "{}"))
    with open(output_file, "w") as f:
        for k in input:
            # Replace `-` with `_` is parameter names since `-` isn't an
            # allowed character for environment variables. cli.py will
            # correctly translate the replaced `-`s.
            f.write(
                "export METAFLOW_INIT_%s=%s\n"
                % (k.upper().replace("-", "_"), json.dumps(input[k]))
            )
    os.chmod(output_file, 509)


if __name__ == "__main__":
    export_parameters(sys.argv[1])
