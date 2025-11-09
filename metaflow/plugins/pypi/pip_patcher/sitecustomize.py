import os
import json

# Because Pip does not offer a direct way to set target platform_system and platform_machine values for resolving packages transitive dependencies, we need to instead
# manually patch the correct target architecture values for pip to be able to resolve the whole dependency tree successfully.
# This is necessary for packages that have conditional dependencies dependent on machine/system, e.g. Torch

try:
    import pip._vendor.packaging.markers as _markers
except ImportError:
    _markers = None

_overrides = os.environ.get("PIP_CUSTOMIZE_OVERRIDES")
if _overrides and _markers:
    _orig_default_environment = _markers.default_environment
    _overrides_dict = json.loads(_overrides)

    def _wrap_default_environment():
        result = _orig_default_environment()
        result.update(_overrides_dict)
        return result

    _markers.default_environment = _wrap_default_environment
