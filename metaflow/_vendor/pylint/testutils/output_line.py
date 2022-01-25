# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import warnings
from typing import Any, NamedTuple, Optional, Sequence, Tuple, TypeVar, Union

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.constants import PY38_PLUS
from metaflow._vendor.pylint.interfaces import UNDEFINED, Confidence
from metaflow._vendor.pylint.message.message import Message
from metaflow._vendor.pylint.testutils.constants import UPDATE_OPTION

T = TypeVar("T")


class MessageTest(NamedTuple):
    msg_id: str
    line: Optional[int] = None
    node: Optional[nodes.NodeNG] = None
    args: Optional[Any] = None
    confidence: Optional[Confidence] = UNDEFINED
    col_offset: Optional[int] = None
    """Used to test messages produced by pylint. Class name cannot start with Test as pytest doesn't allow constructors in test classes."""


class MalformedOutputLineException(Exception):
    def __init__(
        self,
        row: Union[Sequence[str], str],
        exception: Exception,
    ) -> None:
        example = "msg-symbolic-name:42:27:MyClass.my_function:The message"
        other_example = "msg-symbolic-name:7:42::The message"
        expected = [
            "symbol",
            "line",
            "column",
            "end_line",
            "end_column",
            "MyClass.myFunction, (or '')",
            "Message",
            "confidence",
        ]
        reconstructed_row = ""
        i = 0
        try:
            for i, column in enumerate(row):
                reconstructed_row += f"\t{expected[i]}='{column}' ?\n"
            for missing in expected[i + 1 :]:
                reconstructed_row += f"\t{missing}= Nothing provided !\n"
        except IndexError:
            pass
        raw = ":".join(row)
        msg = f"""\
{exception}

Expected '{example}' or '{other_example}' but we got '{raw}':
{reconstructed_row}

Try updating it with: 'python tests/test_functional.py {UPDATE_OPTION}'"""
        super().__init__(msg)


class OutputLine(NamedTuple):
    symbol: str
    lineno: int
    column: int
    end_lineno: Optional[int]
    end_column: Optional[int]
    object: str
    msg: str
    confidence: str

    @classmethod
    def from_msg(cls, msg: Message, check_endline: bool = True) -> "OutputLine":
        """Create an OutputLine from a Pylint Message"""
        column = cls._get_column(msg.column)
        end_line = cls._get_py38_none_value(msg.end_line, check_endline)
        end_column = cls._get_py38_none_value(msg.end_column, check_endline)
        return cls(
            msg.symbol,
            msg.line,
            column,
            end_line,
            end_column,
            msg.obj or "",
            msg.msg.replace("\r\n", "\n"),
            msg.confidence.name,
        )

    @staticmethod
    def _get_column(column: str) -> int:
        """Handle column numbers with the exception of pylint < 3.8 not having them
        in the ast parser.
        """
        if not PY38_PLUS:
            # We check the column only for the new better ast parser introduced in python 3.8
            return 0  # pragma: no cover
        return int(column)

    @staticmethod
    def _get_py38_none_value(value: T, check_endline: bool) -> Optional[T]:
        """Used to make end_line and end_column None as indicated by our version compared to
        `min_pyver_end_position`."""
        if not check_endline:
            return None  # pragma: no cover
        return value

    @classmethod
    def from_csv(
        cls, row: Union[Sequence[str], str], check_endline: bool = True
    ) -> "OutputLine":
        """Create an OutputLine from a comma separated list (the functional tests expected
        output .txt files).
        """
        try:
            if isinstance(row, Sequence):
                column = cls._get_column(row[2])
                if len(row) == 5:
                    warnings.warn(
                        "In pylint 3.0 functional tests expected output should always include the "
                        "expected confidence level, expected end_line and expected end_column. "
                        "An OutputLine should thus have a length of 8.",
                        DeprecationWarning,
                    )
                    return cls(
                        row[0],
                        int(row[1]),
                        column,
                        None,
                        None,
                        row[3],
                        row[4],
                        UNDEFINED.name,
                    )
                if len(row) == 6:
                    warnings.warn(
                        "In pylint 3.0 functional tests expected output should always include the "
                        "expected end_line and expected end_column. An OutputLine should thus have "
                        "a length of 8.",
                        DeprecationWarning,
                    )
                    return cls(
                        row[0], int(row[1]), column, None, None, row[3], row[4], row[5]
                    )
                if len(row) == 8:
                    end_line = cls._get_py38_none_value(row[3], check_endline)
                    end_column = cls._get_py38_none_value(row[4], check_endline)
                    return cls(
                        row[0],
                        int(row[1]),
                        column,
                        cls._value_to_optional_int(end_line),
                        cls._value_to_optional_int(end_column),
                        row[5],
                        row[6],
                        row[7],
                    )
            raise IndexError
        except Exception as e:
            raise MalformedOutputLineException(row, e) from e

    def to_csv(self) -> Tuple[str, str, str, str, str, str, str, str]:
        """Convert an OutputLine to a tuple of string to be written by a
        csv-writer.
        """
        return (
            str(self.symbol),
            str(self.lineno),
            str(self.column),
            str(self.end_lineno),
            str(self.end_column),
            str(self.object),
            str(self.msg),
            str(self.confidence),
        )

    @staticmethod
    def _value_to_optional_int(value: Optional[str]) -> Optional[int]:
        """Checks if a (stringified) value should be None or a Python integer"""
        if value == "None" or not value:
            return None
        return int(value)
