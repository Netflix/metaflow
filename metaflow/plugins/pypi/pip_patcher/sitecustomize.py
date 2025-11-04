import os


# Because Pip does not offer a direct way to set target platform_system and platform_machine values for resolving packages transitive dependencies, we need to instead
# manually patch the correct target architecture values for pip to be able to resolve the whole dependency tree successfully.
# This is necessary for packages that have conditional dependencies dependent on machine/system, e.g. Torch
import platform

platform.system = lambda: os.environ.get("PIP_PATCH_SYSTEM", os.uname().sysname)
platform.machine = lambda: os.environ.get("PIP_PATCH_MACHINE", os.uname().machine)
