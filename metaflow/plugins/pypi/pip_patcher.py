from unittest.mock import patch
import os
import sys


# Because Pip does not offer a direct way to set target platform_system and platform_machine values for resolving packages transitive dependencies, we need to instead
# manually patch the correct target architecture values for pip to be able to resolve the whole dependency tree successfully.
# This is necessary for packages that have conditional dependencies dependent on machine/system, e.g. Torch
@patch(
    "platform.machine", lambda: os.environ.get("PIP_PATCH_MACHINE", os.uname().machine)
)
@patch(
    "platform.system", lambda: os.environ.get("PIP_PATCH_SYSTEM", os.uname().sysname)
)
def _main(args):
    from pip import main

    main(args)


if __name__ == "__main__":
    _main(sys.argv[1:])
