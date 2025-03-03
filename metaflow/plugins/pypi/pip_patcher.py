from unittest.mock import patch
import os
import sys


@patch("platform.machine", lambda: os.environ.get("PIP_PATCH_MACHINE", "x86_64"))
@patch("platform.system", lambda: os.environ.get("PIP_PATCH_SYSTEM", "Linux"))
def _main(args):
    from pip import main

    main(args)


if __name__ == "__main__":
    _main(sys.argv[1:])
