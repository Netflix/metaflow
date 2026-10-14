import sys
import base64
import json
import os
import shlex
from typing import Dict


def parse_parameter_value(base64_value):
    val = base64.b64decode(base64_value).decode("utf-8")

    try:
        return json.loads(val)
    except json.decoder.JSONDecodeError:
        # fallback to using the original value.
        return val


def shell_quote_value(value):
    # The export file is sourced by bash. Strings are written raw so the
    # environment variable holds the value itself rather than its JSON form;
    # everything else is JSON-serialized. Single-quoting keeps bash from
    # word-splitting the value or expanding $(...), `...` and $VAR in it.
    return shlex.quote(value if isinstance(value, str) else json.dumps(value))


def export_parameters(output_file: str, params: Dict[str, str]) -> None:
    with open(output_file, "w", encoding="utf-8") as f:
        for k, v in params.items():
            val, none_default = v
            # Replace `-` with `_` is parameter names since `-` isn't an
            # allowed character for environment variables. cli.py will
            # correctly translate the replaced `-`s.
            parsed_value = parse_parameter_value(val)
            if none_default and parsed_value is None:
                continue
            f.write(
                "export METAFLOW_INIT_%s=%s\n"
                % (
                    k.upper().replace("-", "_"),
                    shell_quote_value(parsed_value),
                )
            )
    os.chmod(output_file, 509)


if __name__ == "__main__":
    try:
        output_filename = sys.argv[1]
    except IndexError:
        raise Exception("an output filename is required.")

    params = {}
    raw_params = sys.argv[2:]

    try:
        for p in raw_params:
            k, req, v = p.split(",")
            none_default = req == "t"
            params[k] = (v, none_default)
    except ValueError:
        raise Exception("Pass in the parameter values as name,default_is_none,value")

    export_parameters(output_filename, params)
