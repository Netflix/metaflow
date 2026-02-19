import sys
import base64
import json


def parse_parameter_value(base64_value):
    val = base64.b64decode(base64_value).decode("utf-8")

    try:
        return json.loads(val)
    except json.decoder.JSONDecodeError:
        # fallback to using the original value.
        return val


if __name__ == "__main__":
    param_name = sys.argv[1]
    default_null = sys.argv[2] == "t"
    base64_val = sys.argv[3]

    val = parse_parameter_value(base64_val)
    if default_null and val == None:
        sys.exit(0)
    else:
        print(f'--{param_name}="{val}"')
