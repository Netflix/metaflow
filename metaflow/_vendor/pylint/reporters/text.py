# Copyright (c) 2006-2007, 2010-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2012-2014 Google, Inc.
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 y2kbugger <y2kbugger@users.noreply.github.com>
# Copyright (c) 2018-2019 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 Jace Browning <jacebrowning@gmail.com>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 bot <bot@noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Plain text reporters:

:text: the default one grouping messages by module
:colorized: an ANSI colorized text reporter
"""
import os
import re
import sys
import warnings
from typing import (
    TYPE_CHECKING,
    Dict,
    NamedTuple,
    Optional,
    Set,
    TextIO,
    Tuple,
    Union,
    cast,
    overload,
)

from metaflow._vendor.pylint.interfaces import IReporter
from metaflow._vendor.pylint.message import Message
from metaflow._vendor.pylint.reporters import BaseReporter
from metaflow._vendor.pylint.reporters.ureports.text_writer import TextWriter
from metaflow._vendor.pylint.utils import _splitstrip

if TYPE_CHECKING:
    from metaflow._vendor.pylint.lint import PyLinter
    from metaflow._vendor.pylint.reporters.ureports.nodes import Section


class MessageStyle(NamedTuple):
    """Styling of a message"""

    color: Optional[str]
    """The color name (see `ANSI_COLORS` for available values)
    or the color number when 256 colors are available
    """
    style: Tuple[str, ...] = ()
    """Tuple of style strings (see `ANSI_COLORS` for available values).
    """


ColorMappingDict = Dict[str, MessageStyle]

TITLE_UNDERLINES = ["", "=", "-", "."]

ANSI_PREFIX = "\033["
ANSI_END = "m"
ANSI_RESET = "\033[0m"
ANSI_STYLES = {
    "reset": "0",
    "bold": "1",
    "italic": "3",
    "underline": "4",
    "blink": "5",
    "inverse": "7",
    "strike": "9",
}
ANSI_COLORS = {
    "reset": "0",
    "black": "30",
    "red": "31",
    "green": "32",
    "yellow": "33",
    "blue": "34",
    "magenta": "35",
    "cyan": "36",
    "white": "37",
}


def _get_ansi_code(msg_style: MessageStyle) -> str:
    """return ansi escape code corresponding to color and style

    :param msg_style: the message style

    :raise KeyError: if an unexistent color or style identifier is given

    :return: the built escape code
    """
    ansi_code = [ANSI_STYLES[effect] for effect in msg_style.style]
    if msg_style.color:
        if msg_style.color.isdigit():
            ansi_code.extend(["38", "5"])
            ansi_code.append(msg_style.color)
        else:
            ansi_code.append(ANSI_COLORS[msg_style.color])
    if ansi_code:
        return ANSI_PREFIX + ";".join(ansi_code) + ANSI_END
    return ""


@overload
def colorize_ansi(
    msg: str,
    msg_style: Optional[MessageStyle] = None,
) -> str:
    ...


@overload
def colorize_ansi(
    msg: str,
    msg_style: Optional[str] = None,
    style: Optional[str] = None,
    *,
    color: Optional[str] = None,
) -> str:
    # Remove for pylint 3.0
    ...


def colorize_ansi(
    msg: str,
    msg_style: Union[MessageStyle, str, None] = None,
    style: Optional[str] = None,
    **kwargs: Optional[str],
) -> str:
    r"""colorize message by wrapping it with ansi escape codes

    :param msg: the message string to colorize

    :param msg_style: the message style
        or color (for backwards compatibility): the color of the message style

    :param style: the message's style elements, this will be deprecated

    :param \**kwargs: used to accept `color` parameter while it is being deprecated

    :return: the ansi escaped string
    """
    # pylint: disable-next=fixme
    # TODO: Remove DeprecationWarning and only accept MessageStyle as parameter
    if not isinstance(msg_style, MessageStyle):
        warnings.warn(
            "In pylint 3.0, the colorize_ansi function of Text reporters will only accept a MessageStyle parameter",
            DeprecationWarning,
        )
        color = kwargs.get("color")
        style_attrs = tuple(_splitstrip(style))
        msg_style = MessageStyle(color or msg_style, style_attrs)
    # If both color and style are not defined, then leave the text as is
    if msg_style.color is None and len(msg_style.style) == 0:
        return msg
    escape_code = _get_ansi_code(msg_style)
    # If invalid (or unknown) color, don't wrap msg with ansi codes
    if escape_code:
        return f"{escape_code}{msg}{ANSI_RESET}"
    return msg


class TextReporter(BaseReporter):
    """Reports messages and layouts in plain text"""

    __implements__ = IReporter
    name = "text"
    extension = "txt"
    line_format = "{path}:{line}:{column}: {msg_id}: {msg} ({symbol})"

    def __init__(self, output: Optional[TextIO] = None) -> None:
        super().__init__(output)
        self._modules: Set[str] = set()
        self._template = self.line_format
        self._fixed_template = self.line_format
        """The output format template with any unrecognized arguments removed"""

    def on_set_current_module(self, module: str, filepath: Optional[str]) -> None:
        """Set the format template to be used and check for unrecognized arguments."""
        template = str(self.linter.config.msg_template or self._template)

        # Return early if the template is the same as the previous one
        if template == self._template:
            return

        # Set template to the currently selected template
        self._template = template

        # Check to see if all parameters in the template are attributes of the Message
        arguments = re.findall(r"\{(.+?)(:.*)?\}", template)
        for argument in arguments:
            if argument[0] not in Message._fields:
                warnings.warn(
                    f"Don't recognize the argument '{argument[0]}' in the --msg-template. "
                    "Are you sure it is supported on the current version of pylint?"
                )
                template = re.sub(r"\{" + argument[0] + r"(:.*?)?\}", "", template)
        self._fixed_template = template

    def write_message(self, msg: Message) -> None:
        """Convenience method to write a formatted message with class default template"""
        self_dict = msg._asdict()
        for key in ("end_line", "end_column"):
            self_dict[key] = self_dict[key] or ""

        self.writeln(self._fixed_template.format(**self_dict))

    def handle_message(self, msg: Message) -> None:
        """manage message of different type and in the context of path"""
        if msg.module not in self._modules:
            if msg.module:
                self.writeln(f"************* Module {msg.module}")
                self._modules.add(msg.module)
            else:
                self.writeln("************* ")
        self.write_message(msg)

    def _display(self, layout: "Section") -> None:
        """launch layouts display"""
        print(file=self.out)
        TextWriter().format(layout, self.out)


class ParseableTextReporter(TextReporter):
    """a reporter very similar to TextReporter, but display messages in a form
    recognized by most text editors :

    <filename>:<linenum>:<msg>
    """

    name = "parseable"
    line_format = "{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}"

    def __init__(self, output: Optional[TextIO] = None) -> None:
        warnings.warn(
            f"{self.name} output format is deprecated. This is equivalent to --msg-template={self.line_format}",
            DeprecationWarning,
        )
        super().__init__(output)


class VSTextReporter(ParseableTextReporter):
    """Visual studio text reporter"""

    name = "msvs"
    line_format = "{path}({line}): [{msg_id}({symbol}){obj}] {msg}"


class ColorizedTextReporter(TextReporter):
    """Simple TextReporter that colorizes text output"""

    name = "colorized"
    COLOR_MAPPING: ColorMappingDict = {
        "I": MessageStyle("green"),
        "C": MessageStyle(None, ("bold",)),
        "R": MessageStyle("magenta", ("bold", "italic")),
        "W": MessageStyle("magenta"),
        "E": MessageStyle("red", ("bold",)),
        "F": MessageStyle("red", ("bold", "underline")),
        "S": MessageStyle("yellow", ("inverse",)),  # S stands for module Separator
    }

    @overload
    def __init__(
        self,
        output: Optional[TextIO] = None,
        color_mapping: Optional[ColorMappingDict] = None,
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        output: Optional[TextIO] = None,
        color_mapping: Optional[Dict[str, Tuple[Optional[str], Optional[str]]]] = None,
    ) -> None:
        # Remove for pylint 3.0
        ...

    def __init__(
        self,
        output: Optional[TextIO] = None,
        color_mapping: Union[
            ColorMappingDict, Dict[str, Tuple[Optional[str], Optional[str]]], None
        ] = None,
    ) -> None:
        super().__init__(output)
        # pylint: disable-next=fixme
        # TODO: Remove DeprecationWarning and only accept ColorMappingDict as color_mapping parameter
        if color_mapping and not isinstance(
            list(color_mapping.values())[0], MessageStyle
        ):
            warnings.warn(
                "In pylint 3.0, the ColoreziedTextReporter will only accept ColorMappingDict as color_mapping parameter",
                DeprecationWarning,
            )
            temp_color_mapping: ColorMappingDict = {}
            for key, value in color_mapping.items():
                color = value[0]
                style_attrs = tuple(_splitstrip(value[1]))
                temp_color_mapping[key] = MessageStyle(color, style_attrs)
            color_mapping = temp_color_mapping
        else:
            color_mapping = cast(Optional[ColorMappingDict], color_mapping)
        self.color_mapping = color_mapping or ColorizedTextReporter.COLOR_MAPPING
        ansi_terms = ["xterm-16color", "xterm-256color"]
        if os.environ.get("TERM") not in ansi_terms:
            if sys.platform == "win32":
                # pylint: disable=import-outside-toplevel
                import colorama

                self.out = colorama.AnsiToWin32(self.out)

    def _get_decoration(self, msg_id: str) -> MessageStyle:
        """Returns the message style as defined in self.color_mapping"""
        return self.color_mapping.get(msg_id[0]) or MessageStyle(None)

    def handle_message(self, msg: Message) -> None:
        """manage message of different types, and colorize output
        using ansi escape codes
        """
        if msg.module not in self._modules:
            msg_style = self._get_decoration("S")
            if msg.module:
                modsep = colorize_ansi(f"************* Module {msg.module}", msg_style)
            else:
                modsep = colorize_ansi(f"************* {msg.module}", msg_style)
            self.writeln(modsep)
            self._modules.add(msg.module)
        msg_style = self._get_decoration(msg.C)

        msg = msg._replace(
            **{
                attr: colorize_ansi(getattr(msg, attr), msg_style)
                for attr in ("msg", "symbol", "category", "C")
            }
        )
        self.write_message(msg)


def register(linter: "PyLinter") -> None:
    """Register the reporter classes with the linter."""
    linter.register_reporter(TextReporter)
    linter.register_reporter(ParseableTextReporter)
    linter.register_reporter(VSTextReporter)
    linter.register_reporter(ColorizedTextReporter)
