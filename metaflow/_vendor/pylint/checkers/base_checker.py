# Copyright (c) 2006-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2013-2014 Google, Inc.
# Copyright (c) 2013 buck@yelp.com <buck@yelp.com>
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Moises Lopez <moylop260@vauxoo.com>
# Copyright (c) 2017-2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2018-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2019 Bruno P. Kinoshita <kinow@users.noreply.github.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 bot <bot@noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE
import functools
from inspect import cleandoc
from typing import Any, Optional

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.config import OptionsProviderMixIn
from metaflow._vendor.pylint.constants import _MSG_ORDER, WarningScope
from metaflow._vendor.pylint.exceptions import InvalidMessageError
from metaflow._vendor.pylint.interfaces import Confidence, IRawChecker, ITokenChecker, implements
from metaflow._vendor.pylint.message.message_definition import MessageDefinition
from metaflow._vendor.pylint.utils import get_rst_section, get_rst_title


@functools.total_ordering
class BaseChecker(OptionsProviderMixIn):

    # checker name (you may reuse an existing one)
    name: str = ""
    # options level (0 will be displaying in --help, 1 in --long-help)
    level = 1
    # ordered list of options to control the checker behaviour
    options: Any = ()
    # messages issued by this checker
    msgs: Any = {}
    # reports issued by this checker
    reports: Any = ()
    # mark this checker as enabled or not.
    enabled: bool = True

    def __init__(self, linter=None):
        """checker instances should have the linter as argument

        :param ILinter linter: is an object implementing ILinter."""
        if self.name is not None:
            self.name = self.name.lower()
        super().__init__()
        self.linter = linter

    def __gt__(self, other):
        """Permit to sort a list of Checker by name."""
        return f"{self.name}{self.msgs}".__gt__(f"{other.name}{other.msgs}")

    def __repr__(self):
        status = "Checker" if self.enabled else "Disabled checker"
        msgs = "', '".join(self.msgs.keys())
        return f"{status} '{self.name}' (responsible for '{msgs}')"

    def __str__(self):
        """This might be incomplete because multiple class inheriting BaseChecker
        can have the same name. Cf MessageHandlerMixIn.get_full_documentation()"""
        return self.get_full_documentation(
            msgs=self.msgs, options=self.options_and_values(), reports=self.reports
        )

    def get_full_documentation(self, msgs, options, reports, doc=None, module=None):
        result = ""
        checker_title = f"{self.name.replace('_', ' ').title()} checker"
        if module:
            # Provide anchor to link against
            result += f".. _{module}:\n\n"
        result += f"{get_rst_title(checker_title, '~')}\n"
        if module:
            result += f"This checker is provided by ``{module}``.\n"
        result += f"Verbatim name of the checker is ``{self.name}``.\n\n"
        if doc:
            # Provide anchor to link against
            result += get_rst_title(f"{checker_title} Documentation", "^")
            result += f"{cleandoc(doc)}\n\n"
        # options might be an empty generator and not be False when casted to boolean
        options = list(options)
        if options:
            result += get_rst_title(f"{checker_title} Options", "^")
            result += f"{get_rst_section(None, options)}\n"
        if msgs:
            result += get_rst_title(f"{checker_title} Messages", "^")
            for msgid, msg in sorted(
                msgs.items(), key=lambda kv: (_MSG_ORDER.index(kv[0][0]), kv[1])
            ):
                msg = self.create_message_definition_from_tuple(msgid, msg)
                result += f"{msg.format_help(checkerref=False)}\n"
            result += "\n"
        if reports:
            result += get_rst_title(f"{checker_title} Reports", "^")
            for report in reports:
                result += (
                    ":%s: %s\n" % report[:2]  # pylint: disable=consider-using-f-string
                )
            result += "\n"
        result += "\n"
        return result

    def add_message(
        self,
        msgid: str,
        line: Optional[int] = None,
        node: Optional[nodes.NodeNG] = None,
        args: Any = None,
        confidence: Optional[Confidence] = None,
        col_offset: Optional[int] = None,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        self.linter.add_message(
            msgid, line, node, args, confidence, col_offset, end_lineno, end_col_offset
        )

    def check_consistency(self):
        """Check the consistency of msgid.

        msg ids for a checker should be a string of len 4, where the two first
        characters are the checker id and the two last the msg id in this
        checker.

        :raises InvalidMessageError: If the checker id in the messages are not
        always the same."""
        checker_id = None
        existing_ids = []
        for message in self.messages:
            if checker_id is not None and checker_id != message.msgid[1:3]:
                error_msg = "Inconsistent checker part in message id "
                error_msg += f"'{message.msgid}' (expected 'x{checker_id}xx' "
                error_msg += f"because we already had {existing_ids})."
                raise InvalidMessageError(error_msg)
            checker_id = message.msgid[1:3]
            existing_ids.append(message.msgid)

    def create_message_definition_from_tuple(self, msgid, msg_tuple):
        if implements(self, (IRawChecker, ITokenChecker)):
            default_scope = WarningScope.LINE
        else:
            default_scope = WarningScope.NODE
        options = {}
        if len(msg_tuple) > 3:
            (msg, symbol, descr, options) = msg_tuple
        elif len(msg_tuple) > 2:
            (msg, symbol, descr) = msg_tuple
        else:
            error_msg = """Messages should have a msgid and a symbol. Something like this :

"W1234": (
    "message",
    "message-symbol",
    "Message description with detail.",
    ...
),
"""
            raise InvalidMessageError(error_msg)
        options.setdefault("scope", default_scope)
        return MessageDefinition(self, msgid, msg, descr, symbol, **options)

    @property
    def messages(self) -> list:
        return [
            self.create_message_definition_from_tuple(msgid, msg_tuple)
            for msgid, msg_tuple in sorted(self.msgs.items())
        ]

    # dummy methods implementing the IChecker interface

    def get_message_definition(self, msgid):
        for message_definition in self.messages:
            if message_definition.msgid == msgid:
                return message_definition
        error_msg = f"MessageDefinition for '{msgid}' does not exists. "
        error_msg += f"Choose from {[m.msgid for m in self.messages]}."
        raise InvalidMessageError(error_msg)

    def open(self):
        """called before visiting project (i.e set of modules)"""

    def close(self):
        """called after visiting project (i.e set of modules)"""


class BaseTokenChecker(BaseChecker):
    """Base class for checkers that want to have access to the token stream."""

    def process_tokens(self, tokens):
        """Should be overridden by subclasses."""
        raise NotImplementedError()
