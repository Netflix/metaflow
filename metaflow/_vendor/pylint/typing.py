# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""A collection of typing utilities."""
import sys
from typing import NamedTuple, Optional, Union

if sys.version_info >= (3, 8):
    from typing import Literal, TypedDict
else:
    from metaflow._vendor.typing_extensions import Literal, TypedDict


class FileItem(NamedTuple):
    """Represents data about a file handled by pylint

    Each file item has:
    - name: full name of the module
    - filepath: path of the file
    - modname: module name
    """

    name: str
    filepath: str
    modpath: str


class ModuleDescriptionDict(TypedDict):
    """Represents data about a checked module"""

    path: str
    name: str
    isarg: bool
    basepath: str
    basename: str


class ErrorDescriptionDict(TypedDict):
    """Represents data about errors collected during checking of a module"""

    key: Literal["fatal"]
    mod: str
    ex: Union[ImportError, SyntaxError]


class MessageLocationTuple(NamedTuple):
    """Tuple with information about the location of a to-be-displayed message"""

    abspath: str
    path: str
    module: str
    obj: str
    line: int
    column: int
    end_line: Optional[int] = None
    end_column: Optional[int] = None


class ManagedMessage(NamedTuple):
    """Tuple with information ahout a managed message of the linter"""

    name: Optional[str]
    msgid: str
    symbol: str
    line: Optional[int]
    is_disabled: bool
