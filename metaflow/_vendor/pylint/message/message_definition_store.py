# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import collections
from typing import TYPE_CHECKING, Dict, List, Tuple, ValuesView

from metaflow._vendor.pylint.exceptions import UnknownMessageError
from metaflow._vendor.pylint.message.message_definition import MessageDefinition
from metaflow._vendor.pylint.message.message_id_store import MessageIdStore

if TYPE_CHECKING:
    from metaflow._vendor.pylint.checkers import BaseChecker


class MessageDefinitionStore:

    """The messages store knows information about every possible message definition but has
    no particular state during analysis.
    """

    def __init__(self) -> None:
        self.message_id_store: MessageIdStore = MessageIdStore()
        # Primary registry for all active messages definitions.
        # It contains the 1:1 mapping from msgid to MessageDefinition.
        # Keys are msgid, values are MessageDefinition
        self._messages_definitions: Dict[str, MessageDefinition] = {}
        # MessageDefinition kept by category
        self._msgs_by_category: Dict[str, List[str]] = collections.defaultdict(list)

    @property
    def messages(self) -> ValuesView[MessageDefinition]:
        """The list of all active messages."""
        return self._messages_definitions.values()

    def register_messages_from_checker(self, checker: "BaseChecker") -> None:
        """Register all messages definitions from a checker."""
        checker.check_consistency()
        for message in checker.messages:
            self.register_message(message)

    def register_message(self, message: MessageDefinition) -> None:
        """Register a MessageDefinition with consistency in mind."""
        self.message_id_store.register_message_definition(
            message.msgid, message.symbol, message.old_names
        )
        self._messages_definitions[message.msgid] = message
        self._msgs_by_category[message.msgid[0]].append(message.msgid)

    def get_message_definitions(self, msgid_or_symbol: str) -> List[MessageDefinition]:
        """Returns the Message definition for either a numeric or symbolic id."""
        return [
            self._messages_definitions[m]
            for m in self.message_id_store.get_active_msgids(msgid_or_symbol)
        ]

    def get_msg_display_string(self, msgid_or_symbol: str) -> str:
        """Generates a user-consumable representation of a message."""
        message_definitions = self.get_message_definitions(msgid_or_symbol)
        if len(message_definitions) == 1:
            return repr(message_definitions[0].symbol)
        return repr([md.symbol for md in message_definitions])

    def help_message(self, msgids_or_symbols: List[str]) -> None:
        """Display help messages for the given message identifiers"""
        for msgids_or_symbol in msgids_or_symbols:
            try:
                for message_definition in self.get_message_definitions(
                    msgids_or_symbol
                ):
                    print(message_definition.format_help(checkerref=True))
                    print("")
            except UnknownMessageError as ex:
                print(ex)
                print("")
                continue

    def list_messages(self) -> None:
        """Output full messages list documentation in ReST format."""
        emittable, non_emittable = self.find_emittable_messages()
        print("Emittable messages with current interpreter:")
        for msg in emittable:
            print(msg.format_help(checkerref=False))
        print("\nNon-emittable messages with current interpreter:")
        for msg in non_emittable:
            print(msg.format_help(checkerref=False))
        print("")

    def find_emittable_messages(
        self,
    ) -> Tuple[List[MessageDefinition], List[MessageDefinition]]:
        """Finds all emittable and non-emittable messages"""
        messages = sorted(self._messages_definitions.values(), key=lambda m: m.msgid)
        emittable = []
        non_emittable = []
        for message in messages:
            if message.may_be_emitted():
                emittable.append(message)
            else:
                non_emittable.append(message)
        return emittable, non_emittable
