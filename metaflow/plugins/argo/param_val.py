import sys
import base64


def parse_parameter_value(base64_value):
    decoded = base64.b64decode(base64_value).decode("utf-8")

    return decoded


if __name__ == "__main__":
    base64_val = sys.argv[1]

    print(parse_parameter_value(base64_val))
