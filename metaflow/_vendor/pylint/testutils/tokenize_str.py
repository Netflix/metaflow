# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import tokenize
from io import StringIO
from tokenize import TokenInfo
from typing import List


def _tokenize_str(code: str) -> List[TokenInfo]:
    return list(tokenize.generate_tokens(StringIO(code).readline))
