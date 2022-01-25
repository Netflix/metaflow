# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE
from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import parse
from metaflow._vendor.astroid.const import PY37_PLUS
from metaflow._vendor.astroid.manager import AstroidManager

if PY37_PLUS:
    # Since Python 3.7 Hashing Methods are added
    # dynamically to globals()

    def _re_transform():
        return parse(
            """
        from collections import namedtuple
        _Method = namedtuple('_Method', 'name ident salt_chars total_size')

        METHOD_SHA512 = _Method('SHA512', '6', 16, 106)
        METHOD_SHA256 = _Method('SHA256', '5', 16, 63)
        METHOD_BLOWFISH = _Method('BLOWFISH', 2, 'b', 22)
        METHOD_MD5 = _Method('MD5', '1', 8, 34)
        METHOD_CRYPT = _Method('CRYPT', None, 2, 13)
        """
        )

    register_module_extender(AstroidManager(), "crypt", _re_transform)
