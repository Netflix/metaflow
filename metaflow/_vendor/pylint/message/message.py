# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE


import collections
from typing import Optional, Tuple, Union, overload
from warnings import warn

from metaflow._vendor.pylint.constants import MSG_TYPES
from metaflow._vendor.pylint.interfaces import Confidence
from metaflow._vendor.pylint.typing import MessageLocationTuple

_MsgBase = collections.namedtuple(
    "_MsgBase",
    [
        "msg_id",
        "symbol",
        "msg",
        "C",
        "category",
        "confidence",
        "abspath",
        "path",
        "module",
        "obj",
        "line",
        "column",
        "end_line",
        "end_column",
    ],
)


class Message(_MsgBase):
    """This class represent a message to be issued by the reporters"""

    @overload
    def __new__(
        cls,
        msg_id: str,
        symbol: str,
        location: MessageLocationTuple,
        msg: str,
        confidence: Optional[Confidence],
    ) -> "Message":
        ...

    @overload
    def __new__(
        cls,
        msg_id: str,
        symbol: str,
        location: Tuple[str, str, str, str, int, int],
        msg: str,
        confidence: Optional[Confidence],
    ) -> "Message":
        # Remove for pylint 3.0
        ...

    def __new__(
        cls,
        msg_id: str,
        symbol: str,
        location: Union[
            Tuple[str, str, str, str, int, int],
            MessageLocationTuple,
        ],
        msg: str,
        confidence: Optional[Confidence],
    ) -> "Message":
        if not isinstance(location, MessageLocationTuple):
            warn(
                "In pylint 3.0, Messages will only accept a MessageLocationTuple as location parameter",
                DeprecationWarning,
            )
            location = location + (None, None)  # type: ignore[assignment] # Temporary fix until deprecation
        return _MsgBase.__new__(
            cls,
            msg_id,
            symbol,
            msg,
            msg_id[0],
            MSG_TYPES[msg_id[0]],
            confidence,
            *location
        )

    def format(self, template: str) -> str:
        """Format the message according to the given template.

        The template format is the one of the format method :
        cf. https://docs.python.org/2/library/string.html#formatstrings
        """
        return template.format(**self._asdict())
