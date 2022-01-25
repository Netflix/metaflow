# Copyright (c) 2006, 2009-2013 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2012-2014 Google, Inc.
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Alexandru Coman <fcoman@bitdefender.com>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2016 glegoux <gilles.legoux@gmail.com>
# Copyright (c) 2017-2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017 Mikhail Fesenko <proggga@gmail.com>
# Copyright (c) 2018 Rogalski, Lukasz <lukasz.rogalski@intel.com>
# Copyright (c) 2018 Lucas Cimon <lucas.cimon@gmail.com>
# Copyright (c) 2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2020 wtracy <afishionado@gmail.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2020 Benny <benny.mueller91@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Konstantina Saketou <56515303+ksaketou@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE


"""Check source code is ascii only or has an encoding declaration (PEP 263)"""

import re
import tokenize
from typing import List, Optional

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker
from metaflow._vendor.pylint.interfaces import IRawChecker, ITokenChecker
from metaflow._vendor.pylint.typing import ManagedMessage
from metaflow._vendor.pylint.utils.pragma_parser import OPTION_PO, PragmaParserError, parse_pragma


class ByIdManagedMessagesChecker(BaseChecker):

    """Checks for messages that are enabled or disabled by id instead of symbol."""

    __implements__ = IRawChecker
    name = "miscellaneous"
    msgs = {
        "I0023": (
            "%s",
            "use-symbolic-message-instead",
            "Used when a message is enabled or disabled by id.",
        )
    }
    options = ()

    def _clear_by_id_managed_msgs(self) -> None:
        self.linter._by_id_managed_msgs.clear()

    def _get_by_id_managed_msgs(self) -> List[ManagedMessage]:
        return self.linter._by_id_managed_msgs

    def process_module(self, node: nodes.Module) -> None:
        """Inspect the source file to find messages activated or deactivated by id."""
        managed_msgs = self._get_by_id_managed_msgs()
        for (mod_name, msgid, symbol, lineno, is_disabled) in managed_msgs:
            if mod_name == node.name:
                verb = "disable" if is_disabled else "enable"
                txt = f"'{msgid}' is cryptic: use '# pylint: {verb}={symbol}' instead"
                self.add_message("use-symbolic-message-instead", line=lineno, args=txt)
        self._clear_by_id_managed_msgs()


class EncodingChecker(BaseChecker):

    """checks for:
    * warning notes in the code like FIXME, XXX
    * encoding issues.
    """

    __implements__ = (IRawChecker, ITokenChecker)

    # configuration section name
    name = "miscellaneous"
    msgs = {
        "W0511": (
            "%s",
            "fixme",
            "Used when a warning note as FIXME or XXX is detected.",
        )
    }

    options = (
        (
            "notes",
            {
                "type": "csv",
                "metavar": "<comma separated values>",
                "default": ("FIXME", "XXX", "TODO"),
                "help": (
                    "List of note tags to take in consideration, "
                    "separated by a comma."
                ),
            },
        ),
        (
            "notes-rgx",
            {
                "type": "string",
                "metavar": "<regexp>",
                "help": "Regular expression of note tags to take in consideration.",
            },
        ),
    )

    def open(self):
        super().open()

        notes = "|".join(re.escape(note) for note in self.config.notes)
        if self.config.notes_rgx:
            regex_string = fr"#\s*({notes}|{self.config.notes_rgx})\b"
        else:
            regex_string = fr"#\s*({notes})\b"

        self._fixme_pattern = re.compile(regex_string, re.I)

    def _check_encoding(
        self, lineno: int, line: bytes, file_encoding: str
    ) -> Optional[str]:
        try:
            return line.decode(file_encoding)
        except UnicodeDecodeError:
            pass
        except LookupError:
            if (
                line.startswith(b"#")
                and "coding" in str(line)
                and file_encoding in str(line)
            ):
                msg = f"Cannot decode using encoding '{file_encoding}', bad encoding"
                self.add_message("syntax-error", line=lineno, args=msg)
        return None

    def process_module(self, node: nodes.Module) -> None:
        """inspect the source file to find encoding problem"""
        encoding = node.file_encoding if node.file_encoding else "ascii"

        with node.stream() as stream:
            for lineno, line in enumerate(stream):
                self._check_encoding(lineno + 1, line, encoding)

    def process_tokens(self, tokens):
        """inspect the source to find fixme problems"""
        if not self.config.notes:
            return
        comments = (
            token_info for token_info in tokens if token_info.type == tokenize.COMMENT
        )
        for comment in comments:
            comment_text = comment.string[1:].lstrip()  # trim '#' and whitespaces

            # handle pylint disable clauses
            disable_option_match = OPTION_PO.search(comment_text)
            if disable_option_match:
                try:
                    values = []
                    try:
                        for pragma_repr in (
                            p_rep
                            for p_rep in parse_pragma(disable_option_match.group(2))
                            if p_rep.action == "disable"
                        ):
                            values.extend(pragma_repr.messages)
                    except PragmaParserError:
                        # Printing useful information dealing with this error is done in the lint package
                        pass
                    if set(values) & set(self.config.notes):
                        continue
                except ValueError:
                    self.add_message(
                        "bad-inline-option",
                        args=disable_option_match.group(1).strip(),
                        line=comment.start[0],
                    )
                    continue

            # emit warnings if necessary
            match = self._fixme_pattern.search("#" + comment_text.lower())
            if match:
                self.add_message(
                    "fixme",
                    col_offset=comment.start[1] + 1,
                    args=comment_text,
                    line=comment.start[0],
                )


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(EncodingChecker(linter))
    linter.register_checker(ByIdManagedMessagesChecker(linter))
