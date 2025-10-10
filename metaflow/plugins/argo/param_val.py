import sys
import base64
import json


def parse_parameter_value(base64_value, param_type):
    val = base64.b64decode(base64_value).decode("utf-8")

    # Keep the value as-is in case of stringified json.
    if param_type == "JSON":
        try:
            # see if param is proper jsonstring
            return json.loads(val)
        except json.decoder.JSONDecodeError:
            # remove outer quotes which conflict with jsonstr
            return val.strip("'")

    try:
        return json.loads(val)
    except json.decoder.JSONDecodeError:
        # fallback to using the original value.
        return val


if __name__ == "__main__":
    base64_val = sys.argv[1]

    try:
        param_type = sys.argv[2]
    except IndexError:
        param_type = "str"

    print(parse_parameter_value(base64_val, param_type))
