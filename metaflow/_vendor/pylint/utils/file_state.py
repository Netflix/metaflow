# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import collections
import sys
from typing import (
    TYPE_CHECKING,
    DefaultDict,
    Dict,
    Iterator,
    Optional,
    Set,
    Tuple,
    Union,
)

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.constants import MSG_STATE_SCOPE_MODULE, WarningScope

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from metaflow._vendor.typing_extensions import Literal

if TYPE_CHECKING:
    from metaflow._vendor.pylint.message import MessageDefinition, MessageDefinitionStore


MessageStateDict = Dict[str, Dict[int, bool]]


class FileState:
    """Hold internal state specific to the currently analyzed file"""

    def __init__(self, modname: Optional[str] = None) -> None:
        self.base_name = modname
        self._module_msgs_state: MessageStateDict = {}
        self._raw_module_msgs_state: MessageStateDict = {}
        self._ignored_msgs: DefaultDict[
            Tuple[str, int], Set[int]
        ] = collections.defaultdict(set)
        self._suppression_mapping: Dict[Tuple[str, int], int] = {}
        self._effective_max_line_number: Optional[int] = None

    def collect_block_lines(
        self, msgs_store: "MessageDefinitionStore", module_node: nodes.Module
    ) -> None:
        """Walk the AST to collect block level options line numbers."""
        for msg, lines in self._module_msgs_state.items():
            self._raw_module_msgs_state[msg] = lines.copy()
        orig_state = self._module_msgs_state.copy()
        self._module_msgs_state = {}
        self._suppression_mapping = {}
        self._effective_max_line_number = module_node.tolineno
        self._collect_block_lines(msgs_store, module_node, orig_state)

    def _collect_block_lines(
        self,
        msgs_store: "MessageDefinitionStore",
        node: nodes.NodeNG,
        msg_state: MessageStateDict,
    ) -> None:
        """Recursively walk (depth first) AST to collect block level options
        line numbers.
        """
        for child in node.get_children():
            self._collect_block_lines(msgs_store, child, msg_state)
        first = node.fromlineno
        last = node.tolineno
        # first child line number used to distinguish between disable
        # which are the first child of scoped node with those defined later.
        # For instance in the code below:
        #
        # 1.   def meth8(self):
        # 2.        """test late disabling"""
        # 3.        pylint: disable=not-callable, useless-suppression
        # 4.        print(self.blip)
        # 5.        pylint: disable=no-member, useless-suppression
        # 6.        print(self.bla)
        #
        # E1102 should be disabled from line 1 to 6 while E1101 from line 5 to 6
        #
        # this is necessary to disable locally messages applying to class /
        # function using their fromlineno
        if (
            isinstance(node, (nodes.Module, nodes.ClassDef, nodes.FunctionDef))
            and node.body
        ):
            firstchildlineno = node.body[0].fromlineno
        else:
            firstchildlineno = last
        for msgid, lines in msg_state.items():
            for lineno, state in list(lines.items()):
                original_lineno = lineno
                if first > lineno or last < lineno:
                    continue
                # Set state for all lines for this block, if the
                # warning is applied to nodes.
                message_definitions = msgs_store.get_message_definitions(msgid)
                for message_definition in message_definitions:
                    if message_definition.scope == WarningScope.NODE:
                        if lineno > firstchildlineno:
                            state = True
                        first_, last_ = node.block_range(lineno)
                    else:
                        first_ = lineno
                        last_ = last
                for line in range(first_, last_ + 1):
                    # do not override existing entries
                    if line in self._module_msgs_state.get(msgid, ()):
                        continue
                    if line in lines:  # state change in the same block
                        state = lines[line]
                        original_lineno = line
                    if not state:
                        self._suppression_mapping[(msgid, line)] = original_lineno
                    try:
                        self._module_msgs_state[msgid][line] = state
                    except KeyError:
                        self._module_msgs_state[msgid] = {line: state}
                del lines[lineno]

    def set_msg_status(self, msg: "MessageDefinition", line: int, status: bool) -> None:
        """Set status (enabled/disable) for a given message at a given line"""
        assert line > 0
        try:
            self._module_msgs_state[msg.msgid][line] = status
        except KeyError:
            self._module_msgs_state[msg.msgid] = {line: status}

    def handle_ignored_message(
        self, state_scope: Optional[Literal[0, 1, 2]], msgid: str, line: int
    ) -> None:
        """Report an ignored message.

        state_scope is either MSG_STATE_SCOPE_MODULE or MSG_STATE_SCOPE_CONFIG,
        depending on whether the message was disabled locally in the module,
        or globally.
        """
        if state_scope == MSG_STATE_SCOPE_MODULE:
            try:
                orig_line = self._suppression_mapping[(msgid, line)]
                self._ignored_msgs[(msgid, orig_line)].add(line)
            except KeyError:
                pass

    def iter_spurious_suppression_messages(
        self,
        msgs_store: "MessageDefinitionStore",
    ) -> Iterator[
        Tuple[
            Literal["useless-suppression", "suppressed-message"],
            int,
            Union[Tuple[str], Tuple[str, int]],
        ]
    ]:
        for warning, lines in self._raw_module_msgs_state.items():
            for line, enable in lines.items():
                if not enable and (warning, line) not in self._ignored_msgs:
                    # ignore cyclic-import check which can show false positives
                    # here due to incomplete context
                    if warning != "R0401":
                        yield "useless-suppression", line, (
                            msgs_store.get_msg_display_string(warning),
                        )
        # don't use iteritems here, _ignored_msgs may be modified by add_message
        for (warning, from_), ignored_lines in list(self._ignored_msgs.items()):
            for line in ignored_lines:
                yield "suppressed-message", line, (
                    msgs_store.get_msg_display_string(warning),
                    from_,
                )

    def get_effective_max_line_number(self) -> Optional[int]:
        return self._effective_max_line_number
