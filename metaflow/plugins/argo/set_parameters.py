import sys
import base64
import json
from typing import Dict


def parse_parameter_value(base64_value):
    val = base64.b64decode(base64_value).decode("utf-8")

    try:
        res = json.loads(val)
    except json.decoder.JSONDecodeError:
        # fallback to using the original value.
        res = val

    return res


def param_opts(params: Dict[str, str]) -> str:
    param_opts = []
    for k, v in params.items():
        val, none_default = v
        parsed_value = parse_parameter_value(val)
        if none_default and parsed_value is None:
            continue
        param_opts.append(f"--{k}={parsed_value}")

    return "\0".join(param_opts)


if __name__ == "__main__":
    params = {}
    raw_params = sys.argv[1:]

    try:
        for p in raw_params:
            k, isnull, v = p.split(",")
            none_default = isnull == "t"
            params[k] = (v, none_default)
    except ValueError:
        raise Exception("Pass in the parameter values as name,default_is_none,value")

    opts = param_opts(params)

    if opts:
        print(opts)
