import os
import json
import shlex
import sys


def shell_quote_value(value):
    # The export file is sourced by bash. Strings are written raw so the
    # environment variable holds the value itself rather than its JSON form;
    # everything else is JSON-serialized. Single-quoting keeps bash from
    # word-splitting the value or expanding $(...), `...` and $VAR in it.
    return shlex.quote(value if isinstance(value, str) else json.dumps(value))


def export_parameters(output_file):
    input = json.loads(os.environ.get("METAFLOW_PARAMETERS", "{}"))
    with open(output_file, "w", encoding="utf-8") as f:
        for k in input:
            # Replace `-` with `_` is parameter names since `-` isn't an
            # allowed character for environment variables. cli.py will
            # correctly translate the replaced `-`s.
            f.write(
                "export METAFLOW_INIT_%s=%s\n"
                % (k.upper().replace("-", "_"), shell_quote_value(input[k]))
            )
    os.chmod(output_file, 509)


if __name__ == "__main__":
    export_parameters(sys.argv[1])
