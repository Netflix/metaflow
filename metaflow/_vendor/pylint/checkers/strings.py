# Copyright (c) 2009-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2010 Daniel Harding <dharding@gmail.com>
# Copyright (c) 2012-2014 Google, Inc.
# Copyright (c) 2013-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Rene Zhang <rz99@cornell.edu>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016, 2018 Jakub Wilk <jwilk@jwilk.net>
# Copyright (c) 2016 Peter Dawyndt <Peter.Dawyndt@UGent.be>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2017 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2018, 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2018-2019 Lucas Cimon <lucas.cimon@gmail.com>
# Copyright (c) 2018 Alan Chan <achan961117@gmail.com>
# Copyright (c) 2018 Yury Gribov <tetra2005@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019 Wes Turner <westurner@google.com>
# Copyright (c) 2019 Djailla <bastien.vallet@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020 Matthew Suozzo <msuozzo@google.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 谭九鼎 <109224573@qq.com>
# Copyright (c) 2020 Anthony <tanant@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Tushar Sadhwani <tushar.sadhwani000@gmail.com>
# Copyright (c) 2021 Jaehoon Hwang <jaehoonhwang@users.noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Peter Kolbus <peter.kolbus@garmin.com>


# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Checker for string formatting operations.
"""

import collections
import numbers
import re
import tokenize
from typing import Counter, Iterable

from metaflow._vendor import astroid
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import BaseChecker, BaseTokenChecker, utils
from metaflow._vendor.pylint.checkers.utils import check_messages
from metaflow._vendor.pylint.interfaces import IAstroidChecker, IRawChecker, ITokenChecker

_AST_NODE_STR_TYPES = ("__builtin__.unicode", "__builtin__.str", "builtins.str")
# Prefixes for both strings and bytes literals per
# https://docs.python.org/3/reference/lexical_analysis.html#string-and-bytes-literals
_PREFIXES = {
    "r",
    "u",
    "R",
    "U",
    "f",
    "F",
    "fr",
    "Fr",
    "fR",
    "FR",
    "rf",
    "rF",
    "Rf",
    "RF",
    "b",
    "B",
    "br",
    "Br",
    "bR",
    "BR",
    "rb",
    "rB",
    "Rb",
    "RB",
}
SINGLE_QUOTED_REGEX = re.compile(f"({'|'.join(_PREFIXES)})?'''")
DOUBLE_QUOTED_REGEX = re.compile(f"({'|'.join(_PREFIXES)})?\"\"\"")
QUOTE_DELIMITER_REGEX = re.compile(f"({'|'.join(_PREFIXES)})?(\"|')", re.DOTALL)

MSGS = {  # pylint: disable=consider-using-namedtuple-or-dataclass
    "E1300": (
        "Unsupported format character %r (%#02x) at index %d",
        "bad-format-character",
        "Used when an unsupported format character is used in a format string.",
    ),
    "E1301": (
        "Format string ends in middle of conversion specifier",
        "truncated-format-string",
        "Used when a format string terminates before the end of a "
        "conversion specifier.",
    ),
    "E1302": (
        "Mixing named and unnamed conversion specifiers in format string",
        "mixed-format-string",
        "Used when a format string contains both named (e.g. '%(foo)d') "
        "and unnamed (e.g. '%d') conversion specifiers.  This is also "
        "used when a named conversion specifier contains * for the "
        "minimum field width and/or precision.",
    ),
    "E1303": (
        "Expected mapping for format string, not %s",
        "format-needs-mapping",
        "Used when a format string that uses named conversion specifiers "
        "is used with an argument that is not a mapping.",
    ),
    "W1300": (
        "Format string dictionary key should be a string, not %s",
        "bad-format-string-key",
        "Used when a format string that uses named conversion specifiers "
        "is used with a dictionary whose keys are not all strings.",
    ),
    "W1301": (
        "Unused key %r in format string dictionary",
        "unused-format-string-key",
        "Used when a format string that uses named conversion specifiers "
        "is used with a dictionary that contains keys not required by the "
        "format string.",
    ),
    "E1304": (
        "Missing key %r in format string dictionary",
        "missing-format-string-key",
        "Used when a format string that uses named conversion specifiers "
        "is used with a dictionary that doesn't contain all the keys "
        "required by the format string.",
    ),
    "E1305": (
        "Too many arguments for format string",
        "too-many-format-args",
        "Used when a format string that uses unnamed conversion "
        "specifiers is given too many arguments.",
    ),
    "E1306": (
        "Not enough arguments for format string",
        "too-few-format-args",
        "Used when a format string that uses unnamed conversion "
        "specifiers is given too few arguments",
    ),
    "E1307": (
        "Argument %r does not match format type %r",
        "bad-string-format-type",
        "Used when a type required by format string "
        "is not suitable for actual argument type",
    ),
    "E1310": (
        "Suspicious argument in %s.%s call",
        "bad-str-strip-call",
        "The argument to a str.{l,r,}strip call contains a duplicate character, ",
    ),
    "W1302": (
        "Invalid format string",
        "bad-format-string",
        "Used when a PEP 3101 format string is invalid.",
    ),
    "W1303": (
        "Missing keyword argument %r for format string",
        "missing-format-argument-key",
        "Used when a PEP 3101 format string that uses named fields "
        "doesn't receive one or more required keywords.",
    ),
    "W1304": (
        "Unused format argument %r",
        "unused-format-string-argument",
        "Used when a PEP 3101 format string that uses named "
        "fields is used with an argument that "
        "is not required by the format string.",
    ),
    "W1305": (
        "Format string contains both automatic field numbering "
        "and manual field specification",
        "format-combined-specification",
        "Used when a PEP 3101 format string contains both automatic "
        "field numbering (e.g. '{}') and manual field "
        "specification (e.g. '{0}').",
    ),
    "W1306": (
        "Missing format attribute %r in format specifier %r",
        "missing-format-attribute",
        "Used when a PEP 3101 format string uses an "
        "attribute specifier ({0.length}), but the argument "
        "passed for formatting doesn't have that attribute.",
    ),
    "W1307": (
        "Using invalid lookup key %r in format specifier %r",
        "invalid-format-index",
        "Used when a PEP 3101 format string uses a lookup specifier "
        "({a[1]}), but the argument passed for formatting "
        "doesn't contain or doesn't have that key as an attribute.",
    ),
    "W1308": (
        "Duplicate string formatting argument %r, consider passing as named argument",
        "duplicate-string-formatting-argument",
        "Used when we detect that a string formatting is "
        "repeating an argument instead of using named string arguments",
    ),
    "W1309": (
        "Using an f-string that does not have any interpolated variables",
        "f-string-without-interpolation",
        "Used when we detect an f-string that does not use any interpolation variables, "
        "in which case it can be either a normal string or a bug in the code.",
    ),
    "W1310": (
        "Using formatting for a string that does not have any interpolated variables",
        "format-string-without-interpolation",
        "Used when we detect a string that does not have any interpolation variables, "
        "in which case it can be either a normal string without formatting or a bug in the code.",
    ),
}

OTHER_NODES = (
    nodes.Const,
    nodes.List,
    nodes.Lambda,
    nodes.FunctionDef,
    nodes.ListComp,
    nodes.SetComp,
    nodes.GeneratorExp,
)


def get_access_path(key, parts):
    """Given a list of format specifiers, returns
    the final access path (e.g. a.b.c[0][1]).
    """
    path = []
    for is_attribute, specifier in parts:
        if is_attribute:
            path.append(f".{specifier}")
        else:
            path.append(f"[{specifier!r}]")
    return str(key) + "".join(path)


def arg_matches_format_type(arg_type, format_type):
    if format_type in "sr":
        # All types can be printed with %s and %r
        return True
    if isinstance(arg_type, astroid.Instance):
        arg_type = arg_type.pytype()
        if arg_type == "builtins.str":
            return format_type == "c"
        if arg_type == "builtins.float":
            return format_type in "deEfFgGn%"
        if arg_type == "builtins.int":
            # Integers allow all types
            return True
        return False
    return True


class StringFormatChecker(BaseChecker):
    """Checks string formatting operations to ensure that the format string
    is valid and the arguments match the format string.
    """

    __implements__ = (IAstroidChecker,)
    name = "string"
    msgs = MSGS

    # pylint: disable=too-many-branches
    @check_messages(
        "bad-format-character",
        "truncated-format-string",
        "mixed-format-string",
        "bad-format-string-key",
        "missing-format-string-key",
        "unused-format-string-key",
        "bad-string-format-type",
        "format-needs-mapping",
        "too-many-format-args",
        "too-few-format-args",
        "bad-string-format-type",
        "format-string-without-interpolation",
    )
    def visit_binop(self, node: nodes.BinOp) -> None:
        if node.op != "%":
            return
        left = node.left
        args = node.right

        if not (isinstance(left, nodes.Const) and isinstance(left.value, str)):
            return
        format_string = left.value
        try:
            (
                required_keys,
                required_num_args,
                required_key_types,
                required_arg_types,
            ) = utils.parse_format_string(format_string)
        except utils.UnsupportedFormatCharacter as exc:
            formatted = format_string[exc.index]
            self.add_message(
                "bad-format-character",
                node=node,
                args=(formatted, ord(formatted), exc.index),
            )
            return
        except utils.IncompleteFormatString:
            self.add_message("truncated-format-string", node=node)
            return
        if not required_keys and not required_num_args:
            self.add_message("format-string-without-interpolation", node=node)
            return
        if required_keys and required_num_args:
            # The format string uses both named and unnamed format
            # specifiers.
            self.add_message("mixed-format-string", node=node)
        elif required_keys:
            # The format string uses only named format specifiers.
            # Check that the RHS of the % operator is a mapping object
            # that contains precisely the set of keys required by the
            # format string.
            if isinstance(args, nodes.Dict):
                keys = set()
                unknown_keys = False
                for k, _ in args.items:
                    if isinstance(k, nodes.Const):
                        key = k.value
                        if isinstance(key, str):
                            keys.add(key)
                        else:
                            self.add_message(
                                "bad-format-string-key", node=node, args=key
                            )
                    else:
                        # One of the keys was something other than a
                        # constant.  Since we can't tell what it is,
                        # suppress checks for missing keys in the
                        # dictionary.
                        unknown_keys = True
                if not unknown_keys:
                    for key in required_keys:
                        if key not in keys:
                            self.add_message(
                                "missing-format-string-key", node=node, args=key
                            )
                for key in keys:
                    if key not in required_keys:
                        self.add_message(
                            "unused-format-string-key", node=node, args=key
                        )
                for key, arg in args.items:
                    if not isinstance(key, nodes.Const):
                        continue
                    format_type = required_key_types.get(key.value, None)
                    arg_type = utils.safe_infer(arg)
                    if (
                        format_type is not None
                        and arg_type
                        and arg_type != astroid.Uninferable
                        and not arg_matches_format_type(arg_type, format_type)
                    ):
                        self.add_message(
                            "bad-string-format-type",
                            node=node,
                            args=(arg_type.pytype(), format_type),
                        )
            elif isinstance(args, (OTHER_NODES, nodes.Tuple)):
                type_name = type(args).__name__
                self.add_message("format-needs-mapping", node=node, args=type_name)
            # else:
            # The RHS of the format specifier is a name or
            # expression.  It may be a mapping object, so
            # there's nothing we can check.
        else:
            # The format string uses only unnamed format specifiers.
            # Check that the number of arguments passed to the RHS of
            # the % operator matches the number required by the format
            # string.
            args_elts = []
            if isinstance(args, nodes.Tuple):
                rhs_tuple = utils.safe_infer(args)
                num_args = None
                if isinstance(rhs_tuple, nodes.BaseContainer):
                    args_elts = rhs_tuple.elts
                    num_args = len(args_elts)
            elif isinstance(args, (OTHER_NODES, (nodes.Dict, nodes.DictComp))):
                args_elts = [args]
                num_args = 1
            else:
                # The RHS of the format specifier is a name or
                # expression.  It could be a tuple of unknown size, so
                # there's nothing we can check.
                num_args = None
            if num_args is not None:
                if num_args > required_num_args:
                    self.add_message("too-many-format-args", node=node)
                elif num_args < required_num_args:
                    self.add_message("too-few-format-args", node=node)
                for arg, format_type in zip(args_elts, required_arg_types):
                    if not arg:
                        continue
                    arg_type = utils.safe_infer(arg)
                    if (
                        arg_type
                        and arg_type != astroid.Uninferable
                        and not arg_matches_format_type(arg_type, format_type)
                    ):
                        self.add_message(
                            "bad-string-format-type",
                            node=node,
                            args=(arg_type.pytype(), format_type),
                        )

    @check_messages("f-string-without-interpolation")
    def visit_joinedstr(self, node: nodes.JoinedStr) -> None:
        self._check_interpolation(node)

    def _check_interpolation(self, node: nodes.JoinedStr) -> None:
        if isinstance(node.parent, nodes.FormattedValue):
            return
        for value in node.values:
            if isinstance(value, nodes.FormattedValue):
                return
        self.add_message("f-string-without-interpolation", node=node)

    @check_messages(*MSGS)
    def visit_call(self, node: nodes.Call) -> None:
        func = utils.safe_infer(node.func)
        if (
            isinstance(func, astroid.BoundMethod)
            and isinstance(func.bound, astroid.Instance)
            and func.bound.name in {"str", "unicode", "bytes"}
        ):
            if func.name in {"strip", "lstrip", "rstrip"} and node.args:
                arg = utils.safe_infer(node.args[0])
                if not isinstance(arg, nodes.Const) or not isinstance(arg.value, str):
                    return
                if len(arg.value) != len(set(arg.value)):
                    self.add_message(
                        "bad-str-strip-call",
                        node=node,
                        args=(func.bound.name, func.name),
                    )
            elif func.name == "format":
                self._check_new_format(node, func)

    def _detect_vacuous_formatting(self, node, positional_arguments):
        counter = collections.Counter(
            arg.name for arg in positional_arguments if isinstance(arg, nodes.Name)
        )
        for name, count in counter.items():
            if count == 1:
                continue
            self.add_message(
                "duplicate-string-formatting-argument", node=node, args=(name,)
            )

    def _check_new_format(self, node, func):
        """Check the new string formatting."""
        # Skip format nodes which don't have an explicit string on the
        # left side of the format operation.
        # We do this because our inference engine can't properly handle
        # redefinitions of the original string.
        # Note that there may not be any left side at all, if the format method
        # has been assigned to another variable. See issue 351. For example:
        #
        #    fmt = 'some string {}'.format
        #    fmt('arg')
        if isinstance(node.func, nodes.Attribute) and not isinstance(
            node.func.expr, nodes.Const
        ):
            return
        if node.starargs or node.kwargs:
            return
        try:
            strnode = next(func.bound.infer())
        except astroid.InferenceError:
            return
        if not (isinstance(strnode, nodes.Const) and isinstance(strnode.value, str)):
            return
        try:
            call_site = astroid.arguments.CallSite.from_call(node)
        except astroid.InferenceError:
            return

        try:
            fields, num_args, manual_pos = utils.parse_format_method_string(
                strnode.value
            )
        except utils.IncompleteFormatString:
            self.add_message("bad-format-string", node=node)
            return

        positional_arguments = call_site.positional_arguments
        named_arguments = call_site.keyword_arguments
        named_fields = {field[0] for field in fields if isinstance(field[0], str)}
        if num_args and manual_pos:
            self.add_message("format-combined-specification", node=node)
            return

        check_args = False
        # Consider "{[0]} {[1]}" as num_args.
        num_args += sum(1 for field in named_fields if field == "")
        if named_fields:
            for field in named_fields:
                if field and field not in named_arguments:
                    self.add_message(
                        "missing-format-argument-key", node=node, args=(field,)
                    )
            for field in named_arguments:
                if field not in named_fields:
                    self.add_message(
                        "unused-format-string-argument", node=node, args=(field,)
                    )
            # num_args can be 0 if manual_pos is not.
            num_args = num_args or manual_pos
            if positional_arguments or num_args:
                empty = any(field == "" for field in named_fields)
                if named_arguments or empty:
                    # Verify the required number of positional arguments
                    # only if the .format got at least one keyword argument.
                    # This means that the format strings accepts both
                    # positional and named fields and we should warn
                    # when one of the them is missing or is extra.
                    check_args = True
        else:
            check_args = True
        if check_args:
            # num_args can be 0 if manual_pos is not.
            num_args = num_args or manual_pos
            if not num_args:
                self.add_message("format-string-without-interpolation", node=node)
                return
            if len(positional_arguments) > num_args:
                self.add_message("too-many-format-args", node=node)
            elif len(positional_arguments) < num_args:
                self.add_message("too-few-format-args", node=node)

        self._detect_vacuous_formatting(node, positional_arguments)
        self._check_new_format_specifiers(node, fields, named_arguments)

    def _check_new_format_specifiers(self, node, fields, named):
        """
        Check attribute and index access in the format
        string ("{0.a}" and "{0[a]}").
        """
        for key, specifiers in fields:
            # Obtain the argument. If it can't be obtained
            # or inferred, skip this check.
            if key == "":
                # {[0]} will have an unnamed argument, defaulting
                # to 0. It will not be present in `named`, so use the value
                # 0 for it.
                key = 0
            if isinstance(key, numbers.Number):
                try:
                    argname = utils.get_argument_from_call(node, key)
                except utils.NoSuchArgumentError:
                    continue
            else:
                if key not in named:
                    continue
                argname = named[key]
            if argname in (astroid.Uninferable, None):
                continue
            try:
                argument = utils.safe_infer(argname)
            except astroid.InferenceError:
                continue
            if not specifiers or not argument:
                # No need to check this key if it doesn't
                # use attribute / item access
                continue
            if argument.parent and isinstance(argument.parent, nodes.Arguments):
                # Ignore any object coming from an argument,
                # because we can't infer its value properly.
                continue
            previous = argument
            parsed = []
            for is_attribute, specifier in specifiers:
                if previous is astroid.Uninferable:
                    break
                parsed.append((is_attribute, specifier))
                if is_attribute:
                    try:
                        previous = previous.getattr(specifier)[0]
                    except astroid.NotFoundError:
                        if (
                            hasattr(previous, "has_dynamic_getattr")
                            and previous.has_dynamic_getattr()
                        ):
                            # Don't warn if the object has a custom __getattr__
                            break
                        path = get_access_path(key, parsed)
                        self.add_message(
                            "missing-format-attribute",
                            args=(specifier, path),
                            node=node,
                        )
                        break
                else:
                    warn_error = False
                    if hasattr(previous, "getitem"):
                        try:
                            previous = previous.getitem(nodes.Const(specifier))
                        except (
                            astroid.AstroidIndexError,
                            astroid.AstroidTypeError,
                            astroid.AttributeInferenceError,
                        ):
                            warn_error = True
                        except astroid.InferenceError:
                            break
                        if previous is astroid.Uninferable:
                            break
                    else:
                        try:
                            # Lookup __getitem__ in the current node,
                            # but skip further checks, because we can't
                            # retrieve the looked object
                            previous.getattr("__getitem__")
                            break
                        except astroid.NotFoundError:
                            warn_error = True
                    if warn_error:
                        path = get_access_path(key, parsed)
                        self.add_message(
                            "invalid-format-index", args=(specifier, path), node=node
                        )
                        break

                try:
                    previous = next(previous.infer())
                except astroid.InferenceError:
                    # can't check further if we can't infer it
                    break


class StringConstantChecker(BaseTokenChecker):
    """Check string literals"""

    __implements__ = (IAstroidChecker, ITokenChecker, IRawChecker)
    name = "string"
    msgs = {
        "W1401": (
            "Anomalous backslash in string: '%s'. "
            "String constant might be missing an r prefix.",
            "anomalous-backslash-in-string",
            "Used when a backslash is in a literal string but not as an escape.",
        ),
        "W1402": (
            "Anomalous Unicode escape in byte string: '%s'. "
            "String constant might be missing an r or u prefix.",
            "anomalous-unicode-escape-in-string",
            "Used when an escape like \\u is encountered in a byte "
            "string where it has no effect.",
        ),
        "W1404": (
            "Implicit string concatenation found in %s",
            "implicit-str-concat",
            "String literals are implicitly concatenated in a "
            "literal iterable definition : "
            "maybe a comma is missing ?",
            {"old_names": [("W1403", "implicit-str-concat-in-sequence")]},
        ),
        "W1405": (
            "Quote delimiter %s is inconsistent with the rest of the file",
            "inconsistent-quotes",
            "Quote delimiters are not used consistently throughout a module "
            "(with allowances made for avoiding unnecessary escaping).",
        ),
        "W1406": (
            "The u prefix for strings is no longer necessary in Python >=3.0",
            "redundant-u-string-prefix",
            "Used when we detect a string with a u prefix. These prefixes were necessary "
            "in Python 2 to indicate a string was Unicode, but since Python 3.0 strings "
            "are Unicode by default.",
        ),
    }
    options = (
        (
            "check-str-concat-over-line-jumps",
            {
                "default": False,
                "type": "yn",
                "metavar": "<y or n>",
                "help": "This flag controls whether the "
                "implicit-str-concat should generate a warning "
                "on implicit string concatenation in sequences defined over "
                "several lines.",
            },
        ),
        (
            "check-quote-consistency",
            {
                "default": False,
                "type": "yn",
                "metavar": "<y or n>",
                "help": "This flag controls whether inconsistent-quotes generates a "
                "warning when the character used as a quote delimiter is used "
                "inconsistently within a module.",
            },
        ),
    )

    # Characters that have a special meaning after a backslash in either
    # Unicode or byte strings.
    ESCAPE_CHARACTERS = "abfnrtvx\n\r\t\\'\"01234567"

    # Characters that have a special meaning after a backslash but only in
    # Unicode strings.
    UNICODE_ESCAPE_CHARACTERS = "uUN"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.string_tokens = {}  # token position -> (token value, next token)

    def process_module(self, node: nodes.Module) -> None:
        self._unicode_literals = "unicode_literals" in node.future_imports

    def process_tokens(self, tokens):
        encoding = "ascii"
        for i, (tok_type, token, start, _, line) in enumerate(tokens):
            if tok_type == tokenize.ENCODING:
                # this is always the first token processed
                encoding = token
            elif tok_type == tokenize.STRING:
                # 'token' is the whole un-parsed token; we can look at the start
                # of it to see whether it's a raw or unicode string etc.
                self.process_string_token(token, start[0], start[1])
                # We figure the next token, ignoring comments & newlines:
                j = i + 1
                while j < len(tokens) and tokens[j].type in (
                    tokenize.NEWLINE,
                    tokenize.NL,
                    tokenize.COMMENT,
                ):
                    j += 1
                next_token = tokens[j] if j < len(tokens) else None
                if encoding != "ascii":
                    # We convert `tokenize` character count into a byte count,
                    # to match with astroid `.col_offset`
                    start = (start[0], len(line[: start[1]].encode(encoding)))
                self.string_tokens[start] = (str_eval(token), next_token)

        if self.config.check_quote_consistency:
            self.check_for_consistent_string_delimiters(tokens)

    @check_messages("implicit-str-concat")
    def visit_list(self, node: nodes.List) -> None:
        self.check_for_concatenated_strings(node.elts, "list")

    @check_messages("implicit-str-concat")
    def visit_set(self, node: nodes.Set) -> None:
        self.check_for_concatenated_strings(node.elts, "set")

    @check_messages("implicit-str-concat")
    def visit_tuple(self, node: nodes.Tuple) -> None:
        self.check_for_concatenated_strings(node.elts, "tuple")

    def visit_assign(self, node: nodes.Assign) -> None:
        if isinstance(node.value, nodes.Const) and isinstance(node.value.value, str):
            self.check_for_concatenated_strings([node.value], "assignment")

    def check_for_consistent_string_delimiters(
        self, tokens: Iterable[tokenize.TokenInfo]
    ) -> None:
        """Adds a message for each string using inconsistent quote delimiters.

        Quote delimiters are used inconsistently if " and ' are mixed in a module's
        shortstrings without having done so to avoid escaping an internal quote
        character.

        Args:
          tokens: The tokens to be checked against for consistent usage.
        """
        string_delimiters: Counter[str] = collections.Counter()

        # First, figure out which quote character predominates in the module
        for tok_type, token, _, _, _ in tokens:
            if tok_type == tokenize.STRING and _is_quote_delimiter_chosen_freely(token):
                string_delimiters[_get_quote_delimiter(token)] += 1

        if len(string_delimiters) > 1:
            # Ties are broken arbitrarily
            most_common_delimiter = string_delimiters.most_common(1)[0][0]
            for tok_type, token, start, _, _ in tokens:
                if tok_type != tokenize.STRING:
                    continue
                quote_delimiter = _get_quote_delimiter(token)
                if (
                    _is_quote_delimiter_chosen_freely(token)
                    and quote_delimiter != most_common_delimiter
                ):
                    self.add_message(
                        "inconsistent-quotes", line=start[0], args=(quote_delimiter,)
                    )

    def check_for_concatenated_strings(self, elements, iterable_type):
        for elt in elements:
            if not (
                isinstance(elt, nodes.Const) and elt.pytype() in _AST_NODE_STR_TYPES
            ):
                continue
            if elt.col_offset < 0:
                # This can happen in case of escaped newlines
                continue
            if (elt.lineno, elt.col_offset) not in self.string_tokens:
                # This may happen with Latin1 encoding
                # cf. https://github.com/PyCQA/pylint/issues/2610
                continue
            matching_token, next_token = self.string_tokens[
                (elt.lineno, elt.col_offset)
            ]
            # We detect string concatenation: the AST Const is the
            # combination of 2 string tokens
            if matching_token != elt.value and next_token is not None:
                if next_token.type == tokenize.STRING and (
                    next_token.start[0] == elt.lineno
                    or self.config.check_str_concat_over_line_jumps
                ):
                    self.add_message(
                        "implicit-str-concat", line=elt.lineno, args=(iterable_type,)
                    )

    def process_string_token(self, token, start_row, start_col):
        quote_char = None
        index = None
        for index, char in enumerate(token):
            if char in "'\"":
                quote_char = char
                break
        if quote_char is None:
            return

        prefix = token[:index].lower()  # markers like u, b, r.
        after_prefix = token[index:]
        # Chop off quotes
        quote_length = (
            3 if after_prefix[:3] == after_prefix[-3:] == 3 * quote_char else 1
        )
        string_body = after_prefix[quote_length:-quote_length]
        # No special checks on raw strings at the moment.
        if "r" not in prefix:
            self.process_non_raw_string_token(
                prefix,
                string_body,
                start_row,
                start_col + len(prefix) + quote_length,
            )

    def process_non_raw_string_token(
        self, prefix, string_body, start_row, string_start_col
    ):
        """check for bad escapes in a non-raw string.

        prefix: lowercase string of eg 'ur' string prefix markers.
        string_body: the un-parsed body of the string, not including the quote
        marks.
        start_row: integer line number in the source.
        string_start_col: integer col number of the string start in the source.
        """
        # Walk through the string; if we see a backslash then escape the next
        # character, and skip over it.  If we see a non-escaped character,
        # alert, and continue.
        #
        # Accept a backslash when it escapes a backslash, or a quote, or
        # end-of-line, or one of the letters that introduce a special escape
        # sequence <https://docs.python.org/reference/lexical_analysis.html>
        #
        index = 0
        while True:
            index = string_body.find("\\", index)
            if index == -1:
                break
            # There must be a next character; having a backslash at the end
            # of the string would be a SyntaxError.
            next_char = string_body[index + 1]
            match = string_body[index : index + 2]
            # The column offset will vary depending on whether the string token
            # is broken across lines. Calculate relative to the nearest line
            # break or relative to the start of the token's line.
            last_newline = string_body.rfind("\n", 0, index)
            if last_newline == -1:
                line = start_row
                col_offset = index + string_start_col
            else:
                line = start_row + string_body.count("\n", 0, index)
                col_offset = index - last_newline - 1
            if next_char in self.UNICODE_ESCAPE_CHARACTERS:
                if "u" in prefix:
                    pass
                elif "b" not in prefix:
                    pass  # unicode by default
                else:
                    self.add_message(
                        "anomalous-unicode-escape-in-string",
                        line=line,
                        args=(match,),
                        col_offset=col_offset,
                    )
            elif next_char not in self.ESCAPE_CHARACTERS:
                self.add_message(
                    "anomalous-backslash-in-string",
                    line=line,
                    args=(match,),
                    col_offset=col_offset,
                )
            # Whether it was a valid escape or not, backslash followed by
            # another character can always be consumed whole: the second
            # character can never be the start of a new backslash escape.
            index += 2

    @check_messages("redundant-u-string-prefix")
    def visit_const(self, node: nodes.Const) -> None:
        if node.pytype() == "builtins.str" and not isinstance(
            node.parent, nodes.JoinedStr
        ):
            self._detect_u_string_prefix(node)

    def _detect_u_string_prefix(self, node: nodes.Const):
        """Check whether strings include a 'u' prefix like u'String'"""
        if node.kind == "u":
            self.add_message(
                "redundant-u-string-prefix",
                line=node.lineno,
                col_offset=node.col_offset,
            )


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(StringFormatChecker(linter))
    linter.register_checker(StringConstantChecker(linter))


def str_eval(token):
    """
    Mostly replicate `ast.literal_eval(token)` manually to avoid any performance hit.
    This supports f-strings, contrary to `ast.literal_eval`.
    We have to support all string literal notations:
    https://docs.python.org/3/reference/lexical_analysis.html#string-and-bytes-literals
    """
    if token[0:2].lower() in {"fr", "rf"}:
        token = token[2:]
    elif token[0].lower() in {"r", "u", "f"}:
        token = token[1:]
    if token[0:3] in {'"""', "'''"}:
        return token[3:-3]
    return token[1:-1]


def _is_long_string(string_token: str) -> bool:
    """Is this string token a "longstring" (is it triple-quoted)?

    Long strings are triple-quoted as defined in
    https://docs.python.org/3/reference/lexical_analysis.html#string-and-bytes-literals

    This function only checks characters up through the open quotes.  Because it's meant
    to be applied only to tokens that represent string literals, it doesn't bother to
    check for close-quotes (demonstrating that the literal is a well-formed string).

    Args:
        string_token: The string token to be parsed.

    Returns:
        A boolean representing whether or not this token matches a longstring
        regex.
    """
    return bool(
        SINGLE_QUOTED_REGEX.match(string_token)
        or DOUBLE_QUOTED_REGEX.match(string_token)
    )


def _get_quote_delimiter(string_token: str) -> str:
    """Returns the quote character used to delimit this token string.

    This function does little checking for whether the token is a well-formed
    string.

    Args:
        string_token: The token to be parsed.

    Returns:
        A string containing solely the first quote delimiter character in the passed
        string.

    Raises:
      ValueError: No quote delimiter characters are present.
    """
    match = QUOTE_DELIMITER_REGEX.match(string_token)
    if not match:
        raise ValueError(f"string token {string_token} is not a well-formed string")
    return match.group(2)


def _is_quote_delimiter_chosen_freely(string_token: str) -> bool:
    """Was there a non-awkward option for the quote delimiter?

    Args:
        string_token: The quoted string whose delimiters are to be checked.

    Returns:
        Whether there was a choice in this token's quote character that would
        not have involved backslash-escaping an interior quote character.  Long
        strings are excepted from this analysis under the assumption that their
        quote characters are set by policy.
    """
    quote_delimiter = _get_quote_delimiter(string_token)
    unchosen_delimiter = '"' if quote_delimiter == "'" else "'"
    return bool(
        quote_delimiter
        and not _is_long_string(string_token)
        and unchosen_delimiter not in str_eval(string_token)
    )
