# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE
from typing import Dict, List, NoReturn, Optional, Tuple

from metaflow._vendor.pylint.exceptions import InvalidMessageError, UnknownMessageError


class MessageIdStore:

    """The MessageIdStore store MessageId and make sure that there is a 1-1 relation between msgid and symbol."""

    def __init__(self) -> None:
        self.__msgid_to_symbol: Dict[str, str] = {}
        self.__symbol_to_msgid: Dict[str, str] = {}
        self.__old_names: Dict[str, List[str]] = {}

    def __len__(self) -> int:
        return len(self.__msgid_to_symbol)

    def __repr__(self) -> str:
        result = "MessageIdStore: [\n"
        for msgid, symbol in self.__msgid_to_symbol.items():
            result += f"  - {msgid} ({symbol})\n"
        result += "]"
        return result

    def get_symbol(self, msgid: str) -> str:
        try:
            return self.__msgid_to_symbol[msgid.upper()]
        except KeyError as e:
            msg = f"'{msgid}' is not stored in the message store."
            raise UnknownMessageError(msg) from e

    def get_msgid(self, symbol: str) -> str:
        try:
            return self.__symbol_to_msgid[symbol]
        except KeyError as e:
            msg = f"'{symbol}' is not stored in the message store."
            raise UnknownMessageError(msg) from e

    def register_message_definition(
        self, msgid: str, symbol: str, old_names: List[Tuple[str, str]]
    ) -> None:
        self.check_msgid_and_symbol(msgid, symbol)
        self.add_msgid_and_symbol(msgid, symbol)
        for old_msgid, old_symbol in old_names:
            self.check_msgid_and_symbol(old_msgid, old_symbol)
            self.add_legacy_msgid_and_symbol(old_msgid, old_symbol, msgid)

    def add_msgid_and_symbol(self, msgid: str, symbol: str) -> None:
        """Add valid message id.

        There is a little duplication with add_legacy_msgid_and_symbol to avoid a function call,
        this is called a lot at initialization."""
        self.__msgid_to_symbol[msgid] = symbol
        self.__symbol_to_msgid[symbol] = msgid

    def add_legacy_msgid_and_symbol(
        self, msgid: str, symbol: str, new_msgid: str
    ) -> None:
        """Add valid legacy message id.

        There is a little duplication with add_msgid_and_symbol to avoid a function call,
        this is called a lot at initialization."""
        self.__msgid_to_symbol[msgid] = symbol
        self.__symbol_to_msgid[symbol] = msgid
        existing_old_names = self.__old_names.get(msgid, [])
        existing_old_names.append(new_msgid)
        self.__old_names[msgid] = existing_old_names

    def check_msgid_and_symbol(self, msgid: str, symbol: str) -> None:
        existing_msgid: Optional[str] = self.__symbol_to_msgid.get(symbol)
        existing_symbol: Optional[str] = self.__msgid_to_symbol.get(msgid)
        if existing_symbol is None and existing_msgid is None:
            return  # both symbol and msgid are usable
        if existing_msgid is not None:
            if existing_msgid != msgid:
                self._raise_duplicate_msgid(symbol, msgid, existing_msgid)
        if existing_symbol and existing_symbol != symbol:
            # See https://github.com/python/mypy/issues/10559
            self._raise_duplicate_symbol(msgid, symbol, existing_symbol)

    @staticmethod
    def _raise_duplicate_symbol(msgid: str, symbol: str, other_symbol: str) -> NoReturn:
        """Raise an error when a symbol is duplicated."""
        symbols = [symbol, other_symbol]
        symbols.sort()
        error_message = f"Message id '{msgid}' cannot have both "
        error_message += f"'{symbols[0]}' and '{symbols[1]}' as symbolic name."
        raise InvalidMessageError(error_message)

    @staticmethod
    def _raise_duplicate_msgid(symbol: str, msgid: str, other_msgid: str) -> NoReturn:
        """Raise an error when a msgid is duplicated."""
        msgids = [msgid, other_msgid]
        msgids.sort()
        error_message = (
            f"Message symbol '{symbol}' cannot be used for "
            f"'{msgids[0]}' and '{msgids[1]}' at the same time."
            f" If you're creating an 'old_names' use 'old-{symbol}' as the old symbol."
        )
        raise InvalidMessageError(error_message)

    def get_active_msgids(self, msgid_or_symbol: str) -> List[str]:
        """Return msgids but the input can be a symbol."""
        # Only msgid can have a digit as second letter
        is_msgid: bool = msgid_or_symbol[1:].isdigit()
        msgid = None
        if is_msgid:
            msgid = msgid_or_symbol.upper()
            symbol = self.__msgid_to_symbol.get(msgid)
        else:
            msgid = self.__symbol_to_msgid.get(msgid_or_symbol)
            symbol = msgid_or_symbol
        if msgid is None or symbol is None or not msgid or not symbol:
            error_msg = f"No such message id or symbol '{msgid_or_symbol}'."
            raise UnknownMessageError(error_msg)
        return self.__old_names.get(msgid, [msgid])
