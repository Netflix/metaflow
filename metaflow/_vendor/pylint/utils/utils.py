# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE


try:
    import isort.api

    HAS_ISORT_5 = True
except ImportError:  # isort < 5
    from metaflow._vendor import isort

    HAS_ISORT_5 = False

import codecs
import os
import re
import sys
import textwrap
import tokenize
from io import BufferedReader, BytesIO
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Pattern,
    TextIO,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from metaflow._vendor.astroid import Module, modutils, nodes

from metaflow._vendor.pylint.constants import PY_EXTS

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from metaflow._vendor.typing_extensions import Literal

if TYPE_CHECKING:
    from metaflow._vendor.pylint.checkers.base_checker import BaseChecker

DEFAULT_LINE_LENGTH = 79

# These are types used to overload get_global_option() and refer to the options type
GLOBAL_OPTION_BOOL = Literal[
    "ignore-mixin-members",
    "suggestion-mode",
    "analyse-fallback-blocks",
    "allow-global-unused-variables",
]
GLOBAL_OPTION_INT = Literal["max-line-length", "docstring-min-length"]
GLOBAL_OPTION_LIST = Literal["ignored-modules"]
GLOBAL_OPTION_PATTERN = Literal[
    "no-docstring-rgx",
    "dummy-variables-rgx",
    "ignored-argument-names",
    "mixin-class-rgx",
]
GLOBAL_OPTION_PATTERN_LIST = Literal["exclude-too-few-public-methods", "ignore-paths"]
GLOBAL_OPTION_TUPLE_INT = Literal["py-version"]
GLOBAL_OPTION_NAMES = Union[
    GLOBAL_OPTION_BOOL,
    GLOBAL_OPTION_INT,
    GLOBAL_OPTION_LIST,
    GLOBAL_OPTION_PATTERN,
    GLOBAL_OPTION_PATTERN_LIST,
    GLOBAL_OPTION_TUPLE_INT,
]
T_GlobalOptionReturnTypes = TypeVar(
    "T_GlobalOptionReturnTypes",
    bool,
    int,
    List[str],
    Pattern[str],
    List[Pattern[str]],
    Tuple[int, ...],
)


def normalize_text(text, line_len=DEFAULT_LINE_LENGTH, indent=""):
    """Wrap the text on the given line length."""
    return "\n".join(
        textwrap.wrap(
            text, width=line_len, initial_indent=indent, subsequent_indent=indent
        )
    )


CMPS = ["=", "-", "+"]


# py3k has no more cmp builtin
def cmp(a, b):
    return (a > b) - (a < b)


def diff_string(old, new):
    """given an old and new int value, return a string representing the
    difference
    """
    diff = abs(old - new)
    diff_str = f"{CMPS[cmp(old, new)]}{diff and f'{diff:.2f}' or ''}"
    return diff_str


def get_module_and_frameid(node):
    """return the module name and the frame id in the module"""
    frame = node.frame()
    module, obj = "", []
    while frame:
        if isinstance(frame, Module):
            module = frame.name
        else:
            obj.append(getattr(frame, "name", "<lambda>"))
        try:
            frame = frame.parent.frame()
        except AttributeError:
            break
    obj.reverse()
    return module, ".".join(obj)


def get_rst_title(title, character):
    """Permit to get a title formatted as ReStructuredText test (underlined with a chosen character)."""
    return f"{title}\n{character * len(title)}\n"


def get_rst_section(section, options, doc=None):
    """format an options section using as a ReStructuredText formatted output"""
    result = ""
    if section:
        result += get_rst_title(section, "'")
    if doc:
        formatted_doc = normalize_text(doc)
        result += f"{formatted_doc}\n\n"
    for optname, optdict, value in options:
        help_opt = optdict.get("help")
        result += f":{optname}:\n"
        if help_opt:
            formatted_help = normalize_text(help_opt, indent="  ")
            result += f"{formatted_help}\n"
        if value and optname != "py-version":
            value = str(_format_option_value(optdict, value))
            result += f"\n  Default: ``{value.replace('`` ', '```` ``')}``\n"
    return result


def decoding_stream(
    stream: Union[BufferedReader, BytesIO],
    encoding: str,
    errors: Literal["strict"] = "strict",
) -> codecs.StreamReader:
    try:
        reader_cls = codecs.getreader(encoding or sys.getdefaultencoding())
    except LookupError:
        reader_cls = codecs.getreader(sys.getdefaultencoding())
    return reader_cls(stream, errors)


def tokenize_module(node: nodes.Module) -> List[tokenize.TokenInfo]:
    with node.stream() as stream:
        readline = stream.readline
        return list(tokenize.tokenize(readline))


def register_plugins(linter, directory):
    """load all module and package in the given directory, looking for a
    'register' function in each one, used to register pylint checkers
    """
    imported = {}
    for filename in os.listdir(directory):
        base, extension = os.path.splitext(filename)
        if base in imported or base == "__pycache__":
            continue
        if (
            extension in PY_EXTS
            and base != "__init__"
            or (
                not extension
                and os.path.isdir(os.path.join(directory, base))
                and not filename.startswith(".")
            )
        ):
            try:
                module = modutils.load_module_from_file(
                    os.path.join(directory, filename)
                )
            except ValueError:
                # empty module name (usually emacs auto-save files)
                continue
            except ImportError as exc:
                print(f"Problem importing module {filename}: {exc}", file=sys.stderr)
            else:
                if hasattr(module, "register"):
                    module.register(linter)
                    imported[base] = 1


@overload
def get_global_option(
    checker: "BaseChecker", option: GLOBAL_OPTION_BOOL, default: Optional[bool] = None
) -> bool:
    ...


@overload
def get_global_option(
    checker: "BaseChecker", option: GLOBAL_OPTION_INT, default: Optional[int] = None
) -> int:
    ...


@overload
def get_global_option(
    checker: "BaseChecker",
    option: GLOBAL_OPTION_LIST,
    default: Optional[List[str]] = None,
) -> List[str]:
    ...


@overload
def get_global_option(
    checker: "BaseChecker",
    option: GLOBAL_OPTION_PATTERN,
    default: Optional[Pattern[str]] = None,
) -> Pattern[str]:
    ...


@overload
def get_global_option(
    checker: "BaseChecker",
    option: GLOBAL_OPTION_PATTERN_LIST,
    default: Optional[List[Pattern[str]]] = None,
) -> List[Pattern[str]]:
    ...


@overload
def get_global_option(
    checker: "BaseChecker",
    option: GLOBAL_OPTION_TUPLE_INT,
    default: Optional[Tuple[int, ...]] = None,
) -> Tuple[int, ...]:
    ...


def get_global_option(
    checker: "BaseChecker",
    option: GLOBAL_OPTION_NAMES,
    default: Optional[T_GlobalOptionReturnTypes] = None,
) -> Optional[T_GlobalOptionReturnTypes]:
    """Retrieve an option defined by the given *checker* or
    by all known option providers.

    It will look in the list of all options providers
    until the given *option* will be found.
    If the option wasn't found, the *default* value will be returned.
    """
    # First, try in the given checker's config.
    # After that, look in the options providers.

    try:
        return getattr(checker.config, option.replace("-", "_"))
    except AttributeError:
        pass
    for provider in checker.linter.options_providers:
        for options in provider.options:
            if options[0] == option:
                return getattr(provider.config, option.replace("-", "_"))
    return default


def _splitstrip(string, sep=","):
    """return a list of stripped string by splitting the string given as
    argument on `sep` (',' by default). Empty string are discarded.

    >>> _splitstrip('a, b, c   ,  4,,')
    ['a', 'b', 'c', '4']
    >>> _splitstrip('a')
    ['a']
    >>> _splitstrip('a,\nb,\nc,')
    ['a', 'b', 'c']

    :type string: str or unicode
    :param string: a csv line

    :type sep: str or unicode
    :param sep: field separator, default to the comma (',')

    :rtype: str or unicode
    :return: the unquoted string (or the input string if it wasn't quoted)
    """
    return [word.strip() for word in string.split(sep) if word.strip()]


def _unquote(string):
    """remove optional quotes (simple or double) from the string

    :type string: str or unicode
    :param string: an optionally quoted string

    :rtype: str or unicode
    :return: the unquoted string (or the input string if it wasn't quoted)
    """
    if not string:
        return string
    if string[0] in "\"'":
        string = string[1:]
    if string[-1] in "\"'":
        string = string[:-1]
    return string


def _check_csv(value):
    if isinstance(value, (list, tuple)):
        return value
    return _splitstrip(value)


def _comment(string: str) -> str:
    """return string as a comment"""
    lines = [line.strip() for line in string.splitlines()]
    sep = "\n"
    return "# " + f"{sep}# ".join(lines)


def _format_option_value(optdict, value):
    """return the user input's value from a 'compiled' value"""
    if optdict.get("type", None) == "py_version":
        value = ".".join(str(item) for item in value)
    elif isinstance(value, (list, tuple)):
        value = ",".join(_format_option_value(optdict, item) for item in value)
    elif isinstance(value, dict):
        value = ",".join(f"{k}:{v}" for k, v in value.items())
    elif hasattr(value, "match"):  # optdict.get('type') == 'regexp'
        # compiled regexp
        value = value.pattern
    elif optdict.get("type") == "yn":
        value = "yes" if value else "no"
    elif isinstance(value, str) and value.isspace():
        value = f"'{value}'"
    return value


def format_section(
    stream: TextIO, section: str, options: List[Tuple], doc: Optional[str] = None
) -> None:
    """format an options section using the INI format"""
    if doc:
        print(_comment(doc), file=stream)
    print(f"[{section}]", file=stream)
    _ini_format(stream, options)


def _ini_format(stream: TextIO, options: List[Tuple]) -> None:
    """format options using the INI format"""
    for optname, optdict, value in options:
        value = _format_option_value(optdict, value)
        help_opt = optdict.get("help")
        if help_opt:
            help_opt = normalize_text(help_opt, indent="# ")
            print(file=stream)
            print(help_opt, file=stream)
        else:
            print(file=stream)
        if value is None:
            print(f"#{optname}=", file=stream)
        else:
            value = str(value).strip()
            if re.match(r"^([\w-]+,)+[\w-]+$", str(value)):
                separator = "\n " + " " * len(optname)
                value = separator.join(x + "," for x in str(value).split(","))
                # remove trailing ',' from last element of the list
                value = value[:-1]
            print(f"{optname}={value}", file=stream)


class IsortDriver:
    """A wrapper around isort API that changed between versions 4 and 5."""

    def __init__(self, config):
        if HAS_ISORT_5:
            self.isort5_config = isort.api.Config(
                # There is not typo here. EXTRA_standard_library is
                # what most users want. The option has been named
                # KNOWN_standard_library for ages in pylint and we
                # don't want to break compatibility.
                extra_standard_library=config.known_standard_library,
                known_third_party=config.known_third_party,
            )
        else:
            self.isort4_obj = isort.SortImports(  # pylint: disable=no-member
                file_contents="",
                known_standard_library=config.known_standard_library,
                known_third_party=config.known_third_party,
            )

    def place_module(self, package):
        if HAS_ISORT_5:
            return isort.api.place_module(package, self.isort5_config)
        return self.isort4_obj.place_module(package)
