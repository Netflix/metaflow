# Copyright (c) 2006-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2012-2015 Google, Inc.
# Copyright (c) 2013 moxian <aleftmail@inbox.ru>
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 frost-nzcr4 <frost.nzcr4@jagmort.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Michal Nowikowski <godfryd@gmail.com>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Mike Frysinger <vapier@gentoo.org>
# Copyright (c) 2015 Fabio Natali <me@fabionatali.com>
# Copyright (c) 2015 Harut <yes@harutune.name>
# Copyright (c) 2015 Mihai Balint <balint.mihai@gmail.com>
# Copyright (c) 2015 Pavel Roskin <proski@gnu.org>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Petr Pulc <petrpulc@gmail.com>
# Copyright (c) 2016 Moises Lopez <moylop260@vauxoo.com>
# Copyright (c) 2016 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2017, 2019-2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017-2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2017 Krzysztof Czapla <k.czapla68@gmail.com>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2017 James M. Allen <james.m.allen@gmail.com>
# Copyright (c) 2017 vinnyrose <vinnyrose@users.noreply.github.com>
# Copyright (c) 2018-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2018, 2020 Bryce Guinta <bryce.guinta@protonmail.com>
# Copyright (c) 2018, 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2018 Lucas Cimon <lucas.cimon@gmail.com>
# Copyright (c) 2018 Michael Hudson-Doyle <michael.hudson@canonical.com>
# Copyright (c) 2018 Natalie Serebryakova <natalie.serebryakova@Natalies-MacBook-Pro.local>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Marcus Näslund <naslundx@gmail.com>
# Copyright (c) 2018 Mike Frysinger <vapier@gmail.com>
# Copyright (c) 2018 Fureigh <rhys.fureigh@gsa.gov>
# Copyright (c) 2018 Andreas Freimuth <andreas.freimuth@united-bits.de>
# Copyright (c) 2018 Jakub Wilk <jwilk@jwilk.net>
# Copyright (c) 2019 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020 Raphael Gaschignard <raphael@rtpg.co>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Tushar Sadhwani <tushar.sadhwani000@gmail.com>
# Copyright (c) 2021 bot <bot@noreply.github.com>
# Copyright (c) 2021 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Python code format's checker.

By default try to follow Guido's style guide :

https://www.python.org/doc/essays/styleguide/

Some parts of the process_token method is based from The Tab Nanny std module.
"""

import tokenize
from functools import reduce
from typing import List

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseTokenChecker
from metaflow._vendor.pylint.checkers.utils import (
    check_messages,
    is_overload_stub,
    is_protocol_class,
    node_frame_class,
)
from metaflow._vendor.pylint.constants import WarningScope
from metaflow._vendor.pylint.interfaces import IAstroidChecker, IRawChecker, ITokenChecker
from metaflow._vendor.pylint.utils.pragma_parser import OPTION_PO, PragmaParserError, parse_pragma

_ASYNC_TOKEN = "async"
_KEYWORD_TOKENS = [
    "assert",
    "del",
    "elif",
    "except",
    "for",
    "if",
    "in",
    "not",
    "raise",
    "return",
    "while",
    "yield",
    "with",
]

_SPACED_OPERATORS = [
    "==",
    "<",
    ">",
    "!=",
    "<>",
    "<=",
    ">=",
    "+=",
    "-=",
    "*=",
    "**=",
    "/=",
    "//=",
    "&=",
    "|=",
    "^=",
    "%=",
    ">>=",
    "<<=",
]
_OPENING_BRACKETS = ["(", "[", "{"]
_CLOSING_BRACKETS = [")", "]", "}"]
_TAB_LENGTH = 8

_EOL = frozenset([tokenize.NEWLINE, tokenize.NL, tokenize.COMMENT])
_JUNK_TOKENS = (tokenize.COMMENT, tokenize.NL)

# Whitespace checking policy constants
_MUST = 0
_MUST_NOT = 1
_IGNORE = 2

MSGS = {
    "C0301": (
        "Line too long (%s/%s)",
        "line-too-long",
        "Used when a line is longer than a given number of characters.",
    ),
    "C0302": (
        "Too many lines in module (%s/%s)",  # was W0302
        "too-many-lines",
        "Used when a module has too many lines, reducing its readability.",
    ),
    "C0303": (
        "Trailing whitespace",
        "trailing-whitespace",
        "Used when there is whitespace between the end of a line and the newline.",
    ),
    "C0304": (
        "Final newline missing",
        "missing-final-newline",
        "Used when the last line in a file is missing a newline.",
    ),
    "C0305": (
        "Trailing newlines",
        "trailing-newlines",
        "Used when there are trailing blank lines in a file.",
    ),
    "W0311": (
        "Bad indentation. Found %s %s, expected %s",
        "bad-indentation",
        "Used when an unexpected number of indentation's tabulations or "
        "spaces has been found.",
    ),
    "W0301": (
        "Unnecessary semicolon",  # was W0106
        "unnecessary-semicolon",
        'Used when a statement is ended by a semi-colon (";"), which '
        "isn't necessary (that's python, not C ;).",
    ),
    "C0321": (
        "More than one statement on a single line",
        "multiple-statements",
        "Used when more than on statement are found on the same line.",
        {"scope": WarningScope.NODE},
    ),
    "C0325": (
        "Unnecessary parens after %r keyword",
        "superfluous-parens",
        "Used when a single item in parentheses follows an if, for, or "
        "other keyword.",
    ),
    "C0327": (
        "Mixed line endings LF and CRLF",
        "mixed-line-endings",
        "Used when there are mixed (LF and CRLF) newline signs in a file.",
    ),
    "C0328": (
        "Unexpected line ending format. There is '%s' while it should be '%s'.",
        "unexpected-line-ending-format",
        "Used when there is different newline than expected.",
    ),
}


def _last_token_on_line_is(tokens, line_end, token):
    return (
        line_end > 0
        and tokens.token(line_end - 1) == token
        or line_end > 1
        and tokens.token(line_end - 2) == token
        and tokens.type(line_end - 1) == tokenize.COMMENT
    )


# The contexts for hanging indents.
# A hanging indented dictionary value after :
HANGING_DICT_VALUE = "dict-value"
# Hanging indentation in an expression.
HANGING = "hanging"
# Hanging indentation in a block header.
HANGING_BLOCK = "hanging-block"
# Continued indentation inside an expression.
CONTINUED = "continued"
# Continued indentation in a block header.
CONTINUED_BLOCK = "continued-block"

SINGLE_LINE = "single"
WITH_BODY = "multi"


class TokenWrapper:
    """A wrapper for readable access to token information."""

    def __init__(self, tokens):
        self._tokens = tokens

    def token(self, idx):
        return self._tokens[idx][1]

    def type(self, idx):
        return self._tokens[idx][0]

    def start_line(self, idx):
        return self._tokens[idx][2][0]

    def start_col(self, idx):
        return self._tokens[idx][2][1]

    def line(self, idx):
        return self._tokens[idx][4]


class FormatChecker(BaseTokenChecker):
    """checks for :
    * unauthorized constructions
    * strict indentation
    * line length
    """

    __implements__ = (ITokenChecker, IAstroidChecker, IRawChecker)

    # configuration section name
    name = "format"
    # messages
    msgs = MSGS
    # configuration options
    # for available dict keys/values see the optik parser 'add_option' method
    options = (
        (
            "max-line-length",
            {
                "default": 100,
                "type": "int",
                "metavar": "<int>",
                "help": "Maximum number of characters on a single line.",
            },
        ),
        (
            "ignore-long-lines",
            {
                "type": "regexp",
                "metavar": "<regexp>",
                "default": r"^\s*(# )?<?https?://\S+>?$",
                "help": (
                    "Regexp for a line that is allowed to be longer than the limit."
                ),
            },
        ),
        (
            "single-line-if-stmt",
            {
                "default": False,
                "type": "yn",
                "metavar": "<y or n>",
                "help": (
                    "Allow the body of an if to be on the same "
                    "line as the test if there is no else."
                ),
            },
        ),
        (
            "single-line-class-stmt",
            {
                "default": False,
                "type": "yn",
                "metavar": "<y or n>",
                "help": (
                    "Allow the body of a class to be on the same "
                    "line as the declaration if body contains "
                    "single statement."
                ),
            },
        ),
        (
            "max-module-lines",
            {
                "default": 1000,
                "type": "int",
                "metavar": "<int>",
                "help": "Maximum number of lines in a module.",
            },
        ),
        (
            "indent-string",
            {
                "default": "    ",
                "type": "non_empty_string",
                "metavar": "<string>",
                "help": "String used as indentation unit. This is usually "
                '"    " (4 spaces) or "\\t" (1 tab).',
            },
        ),
        (
            "indent-after-paren",
            {
                "type": "int",
                "metavar": "<int>",
                "default": 4,
                "help": "Number of spaces of indent required inside a hanging "
                "or continued line.",
            },
        ),
        (
            "expected-line-ending-format",
            {
                "type": "choice",
                "metavar": "<empty or LF or CRLF>",
                "default": "",
                "choices": ["", "LF", "CRLF"],
                "help": (
                    "Expected format of line ending, "
                    "e.g. empty (any line ending), LF or CRLF."
                ),
            },
        ),
    )

    def __init__(self, linter=None):
        super().__init__(linter)
        self._lines = None
        self._visited_lines = None
        self._bracket_stack = [None]

    def new_line(self, tokens, line_end, line_start):
        """a new line has been encountered, process it if necessary"""
        if _last_token_on_line_is(tokens, line_end, ";"):
            self.add_message("unnecessary-semicolon", line=tokens.start_line(line_end))

        line_num = tokens.start_line(line_start)
        line = tokens.line(line_start)
        if tokens.type(line_start) not in _JUNK_TOKENS:
            self._lines[line_num] = line.split("\n")[0]
        self.check_lines(line, line_num)

    def process_module(self, _node: nodes.Module) -> None:
        pass

    def _check_keyword_parentheses(
        self, tokens: List[tokenize.TokenInfo], start: int
    ) -> None:
        """Check that there are not unnecessary parentheses after a keyword.

        Parens are unnecessary if there is exactly one balanced outer pair on a
        line, and it is followed by a colon, and contains no commas (i.e. is not a
        tuple).

        Args:
        tokens: list of Tokens; the entire list of Tokens.
        start: int; the position of the keyword in the token list.
        """
        # If the next token is not a paren, we're fine.
        if self._bracket_stack[-1] == ":" and tokens[start].string == "for":
            self._bracket_stack.pop()
        if tokens[start + 1].string != "(":
            return
        found_and_or = False
        contains_walrus_operator = False
        walrus_operator_depth = 0
        contains_double_parens = 0
        depth = 0
        keyword_token = str(tokens[start].string)
        line_num = tokens[start].start[0]
        for i in range(start, len(tokens) - 1):
            token = tokens[i]

            # If we hit a newline, then assume any parens were for continuation.
            if token.type == tokenize.NL:
                return
            # Since the walrus operator doesn't exist below python3.8, the tokenizer
            # generates independent tokens
            if (
                token.string == ":="  # <-- python3.8+ path
                or token.string + tokens[i + 1].string == ":="
            ):
                contains_walrus_operator = True
                walrus_operator_depth = depth
            if token.string == "(":
                depth += 1
                if tokens[i + 1].string == "(":
                    contains_double_parens = 1
            elif token.string == ")":
                depth -= 1
                if depth:
                    if contains_double_parens and tokens[i + 1].string == ")":
                        # For walrus operators in `if (not)` conditions and comprehensions
                        if keyword_token in {"in", "if", "not"}:
                            continue
                        return
                    contains_double_parens -= 1
                    continue
                # ')' can't happen after if (foo), since it would be a syntax error.
                if tokens[i + 1].string in {":", ")", "]", "}", "in"} or tokens[
                    i + 1
                ].type in {tokenize.NEWLINE, tokenize.ENDMARKER, tokenize.COMMENT}:
                    if contains_walrus_operator and walrus_operator_depth - 1 == depth:
                        return
                    # The empty tuple () is always accepted.
                    if i == start + 2:
                        return
                    if keyword_token == "not":
                        if not found_and_or:
                            self.add_message(
                                "superfluous-parens", line=line_num, args=keyword_token
                            )
                    elif keyword_token in {"return", "yield"}:
                        self.add_message(
                            "superfluous-parens", line=line_num, args=keyword_token
                        )
                    elif not found_and_or and keyword_token != "in":
                        self.add_message(
                            "superfluous-parens", line=line_num, args=keyword_token
                        )
                return
            elif depth == 1:
                # This is a tuple, which is always acceptable.
                if token[1] == ",":
                    return
                # 'and' and 'or' are the only boolean operators with lower precedence
                # than 'not', so parens are only required when they are found.
                if token[1] in {"and", "or"}:
                    found_and_or = True
                # A yield inside an expression must always be in parentheses,
                # quit early without error.
                elif token[1] == "yield":
                    return
                # A generator expression always has a 'for' token in it, and
                # the 'for' token is only legal inside parens when it is in a
                # generator expression.  The parens are necessary here, so bail
                # without an error.
                elif token[1] == "for":
                    return
                # A generator expression can have an 'else' token in it.
                # We check the rest of the tokens to see if any problems incure after
                # the 'else'.
                elif token[1] == "else":
                    if "(" in (i.string for i in tokens[i:]):
                        self._check_keyword_parentheses(tokens[i:], 0)
                    return

    def _prepare_token_dispatcher(self):
        dispatch = {}
        for tokens, handler in ((_KEYWORD_TOKENS, self._check_keyword_parentheses),):
            for token in tokens:
                dispatch[token] = handler
        return dispatch

    def process_tokens(self, tokens):
        """process tokens and search for :

        _ too long lines (i.e. longer than <max_chars>)
        _ optionally bad construct (if given, bad_construct must be a compiled
          regular expression).
        """
        self._bracket_stack = [None]
        indents = [0]
        check_equal = False
        line_num = 0
        self._lines = {}
        self._visited_lines = {}
        token_handlers = self._prepare_token_dispatcher()
        self._last_line_ending = None
        last_blank_line_num = 0
        for idx, (tok_type, token, start, _, line) in enumerate(tokens):
            if start[0] != line_num:
                line_num = start[0]
                # A tokenizer oddity: if an indented line contains a multi-line
                # docstring, the line member of the INDENT token does not contain
                # the full line; therefore we check the next token on the line.
                if tok_type == tokenize.INDENT:
                    self.new_line(TokenWrapper(tokens), idx - 1, idx + 1)
                else:
                    self.new_line(TokenWrapper(tokens), idx - 1, idx)

            if tok_type == tokenize.NEWLINE:
                # a program statement, or ENDMARKER, will eventually follow,
                # after some (possibly empty) run of tokens of the form
                #     (NL | COMMENT)* (INDENT | DEDENT+)?
                # If an INDENT appears, setting check_equal is wrong, and will
                # be undone when we see the INDENT.
                check_equal = True
                self._check_line_ending(token, line_num)
            elif tok_type == tokenize.INDENT:
                check_equal = False
                self.check_indent_level(token, indents[-1] + 1, line_num)
                indents.append(indents[-1] + 1)
            elif tok_type == tokenize.DEDENT:
                # there's nothing we need to check here!  what's important is
                # that when the run of DEDENTs ends, the indentation of the
                # program statement (or ENDMARKER) that triggered the run is
                # equal to what's left at the top of the indents stack
                check_equal = True
                if len(indents) > 1:
                    del indents[-1]
            elif tok_type == tokenize.NL:
                if not line.strip("\r\n"):
                    last_blank_line_num = line_num
            elif tok_type not in (tokenize.COMMENT, tokenize.ENCODING):
                # This is the first concrete token following a NEWLINE, so it
                # must be the first token of the next program statement, or an
                # ENDMARKER; the "line" argument exposes the leading whitespace
                # for this statement; in the case of ENDMARKER, line is an empty
                # string, so will properly match the empty string with which the
                # "indents" stack was seeded
                if check_equal:
                    check_equal = False
                    self.check_indent_level(line, indents[-1], line_num)

            if tok_type == tokenize.NUMBER and token.endswith("l"):
                self.add_message("lowercase-l-suffix", line=line_num)

            try:
                handler = token_handlers[token]
            except KeyError:
                pass
            else:
                handler(tokens, idx)

        line_num -= 1  # to be ok with "wc -l"
        if line_num > self.config.max_module_lines:
            # Get the line where the too-many-lines (or its message id)
            # was disabled or default to 1.
            message_definition = self.linter.msgs_store.get_message_definitions(
                "too-many-lines"
            )[0]
            names = (message_definition.msgid, "too-many-lines")
            line = next(
                filter(None, (self.linter._pragma_lineno.get(name) for name in names)),
                1,
            )
            self.add_message(
                "too-many-lines",
                args=(line_num, self.config.max_module_lines),
                line=line,
            )

        # See if there are any trailing lines.  Do not complain about empty
        # files like __init__.py markers.
        if line_num == last_blank_line_num and line_num > 0:
            self.add_message("trailing-newlines", line=line_num)

    def _check_line_ending(self, line_ending, line_num):
        # check if line endings are mixed
        if self._last_line_ending is not None:
            # line_ending == "" indicates a synthetic newline added at
            # the end of a file that does not, in fact, end with a
            # newline.
            if line_ending and line_ending != self._last_line_ending:
                self.add_message("mixed-line-endings", line=line_num)

        self._last_line_ending = line_ending

        # check if line ending is as expected
        expected = self.config.expected_line_ending_format
        if expected:
            # reduce multiple \n\n\n\n to one \n
            line_ending = reduce(lambda x, y: x + y if x != y else x, line_ending, "")
            line_ending = "LF" if line_ending == "\n" else "CRLF"
            if line_ending != expected:
                self.add_message(
                    "unexpected-line-ending-format",
                    args=(line_ending, expected),
                    line=line_num,
                )

    @check_messages("multiple-statements")
    def visit_default(self, node: nodes.NodeNG) -> None:
        """check the node line number and check it if not yet done"""
        if not node.is_statement:
            return
        if not node.root().pure_python:
            return
        prev_sibl = node.previous_sibling()
        if prev_sibl is not None:
            prev_line = prev_sibl.fromlineno
        # The line on which a finally: occurs in a try/finally
        # is not directly represented in the AST. We infer it
        # by taking the last line of the body and adding 1, which
        # should be the line of finally:
        elif (
            isinstance(node.parent, nodes.TryFinally) and node in node.parent.finalbody
        ):
            prev_line = node.parent.body[0].tolineno + 1
        else:
            prev_line = node.parent.statement().fromlineno
        line = node.fromlineno
        assert line, node
        if prev_line == line and self._visited_lines.get(line) != 2:
            self._check_multi_statement_line(node, line)
            return
        if line in self._visited_lines:
            return
        try:
            tolineno = node.blockstart_tolineno
        except AttributeError:
            tolineno = node.tolineno
        assert tolineno, node
        lines = []
        for line in range(line, tolineno + 1):
            self._visited_lines[line] = 1
            try:
                lines.append(self._lines[line].rstrip())
            except KeyError:
                lines.append("")

    def _check_multi_statement_line(self, node, line):
        """Check for lines containing multiple statements."""
        # Do not warn about multiple nested context managers
        # in with statements.
        if isinstance(node, nodes.With):
            return
        # For try... except... finally..., the two nodes
        # appear to be on the same line due to how the AST is built.
        if isinstance(node, nodes.TryExcept) and isinstance(
            node.parent, nodes.TryFinally
        ):
            return
        if (
            isinstance(node.parent, nodes.If)
            and not node.parent.orelse
            and self.config.single_line_if_stmt
        ):
            return
        if (
            isinstance(node.parent, nodes.ClassDef)
            and len(node.parent.body) == 1
            and self.config.single_line_class_stmt
        ):
            return

        # Function overloads that use ``Ellipsis`` are exempted.
        if (
            isinstance(node, nodes.Expr)
            and isinstance(node.value, nodes.Const)
            and node.value.value is Ellipsis
        ):
            frame = node.frame()
            if is_overload_stub(frame) or is_protocol_class(node_frame_class(frame)):
                return

        self.add_message("multiple-statements", node=node)
        self._visited_lines[line] = 2

    def check_line_ending(self, line: str, i: int) -> None:
        """
        Check that the final newline is not missing and that there is no trailing whitespace.
        """
        if not line.endswith("\n"):
            self.add_message("missing-final-newline", line=i)
            return
        # exclude \f (formfeed) from the rstrip
        stripped_line = line.rstrip("\t\n\r\v ")
        if line[len(stripped_line) :] not in ("\n", "\r\n"):
            self.add_message(
                "trailing-whitespace", line=i, col_offset=len(stripped_line)
            )

    def check_line_length(self, line: str, i: int, checker_off: bool) -> None:
        """
        Check that the line length is less than the authorized value
        """
        max_chars = self.config.max_line_length
        ignore_long_line = self.config.ignore_long_lines
        line = line.rstrip()
        if len(line) > max_chars and not ignore_long_line.search(line):
            if checker_off:
                self.linter.add_ignored_message("line-too-long", i)
            else:
                self.add_message("line-too-long", line=i, args=(len(line), max_chars))

    @staticmethod
    def remove_pylint_option_from_lines(options_pattern_obj) -> str:
        """
        Remove the `# pylint ...` pattern from lines
        """
        lines = options_pattern_obj.string
        purged_lines = (
            lines[: options_pattern_obj.start(1)].rstrip()
            + lines[options_pattern_obj.end(1) :]
        )
        return purged_lines

    @staticmethod
    def is_line_length_check_activated(pylint_pattern_match_object) -> bool:
        """
        Return true if the line length check is activated
        """
        try:
            for pragma in parse_pragma(pylint_pattern_match_object.group(2)):
                if pragma.action == "disable" and "line-too-long" in pragma.messages:
                    return False
        except PragmaParserError:
            # Printing useful information dealing with this error is done in the lint package
            pass
        return True

    @staticmethod
    def specific_splitlines(lines: str) -> List[str]:
        """
        Split lines according to universal newlines except those in a specific sets
        """
        unsplit_ends = {
            "\v",
            "\x0b",
            "\f",
            "\x0c",
            "\x1c",
            "\x1d",
            "\x1e",
            "\x85",
            "\u2028",
            "\u2029",
        }
        res = []
        buffer = ""
        for atomic_line in lines.splitlines(True):
            if atomic_line[-1] not in unsplit_ends:
                res.append(buffer + atomic_line)
                buffer = ""
            else:
                buffer += atomic_line
        return res

    def check_lines(self, lines: str, lineno: int) -> None:
        """
        Check lines have :
            - a final newline
            - no trailing whitespace
            - less than a maximum number of characters
        """
        # we're first going to do a rough check whether any lines in this set
        # go over the line limit. If none of them do, then we don't need to
        # parse out the pylint options later on and can just assume that these
        # lines are clean

        # we'll also handle the line ending check here to avoid double-iteration
        # unless the line lengths are suspect

        max_chars = self.config.max_line_length

        split_lines = self.specific_splitlines(lines)

        for offset, line in enumerate(split_lines):
            self.check_line_ending(line, lineno + offset)

        # hold onto the initial lineno for later
        potential_line_length_warning = False
        for offset, line in enumerate(split_lines):
            # this check is purposefully simple and doesn't rstrip
            # since this is running on every line you're checking it's
            # advantageous to avoid doing a lot of work
            if len(line) > max_chars:
                potential_line_length_warning = True
                break

        # if there were no lines passing the max_chars config, we don't bother
        # running the full line check (as we've met an even more strict condition)
        if not potential_line_length_warning:
            return

        # Line length check may be deactivated through `pylint: disable` comment
        mobj = OPTION_PO.search(lines)
        checker_off = False
        if mobj:
            if not self.is_line_length_check_activated(mobj):
                checker_off = True
            # The 'pylint: disable whatever' should not be taken into account for line length count
            lines = self.remove_pylint_option_from_lines(mobj)

        # here we re-run specific_splitlines since we have filtered out pylint options above
        for offset, line in enumerate(self.specific_splitlines(lines)):
            self.check_line_length(line, lineno + offset, checker_off)

    def check_indent_level(self, string, expected, line_num):
        """return the indent level of the string"""
        indent = self.config.indent_string
        if indent == "\\t":  # \t is not interpreted in the configuration file
            indent = "\t"
        level = 0
        unit_size = len(indent)
        while string[:unit_size] == indent:
            string = string[unit_size:]
            level += 1
        suppl = ""
        while string and string[0] in " \t":
            suppl += string[0]
            string = string[1:]
        if level != expected or suppl:
            i_type = "spaces"
            if indent[0] == "\t":
                i_type = "tabs"
            self.add_message(
                "bad-indentation",
                line=line_num,
                args=(level * unit_size + len(suppl), i_type, expected * unit_size),
            )


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(FormatChecker(linter))
