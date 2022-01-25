# Copyright (c) 2016-2019, 2021 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2016-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2016 Yuri Bochkarev <baltazar.bz@gmail.com>
# Copyright (c) 2016 Glenn Matthews <glenn@e-dad.net>
# Copyright (c) 2016 Moises Lopez <moylop260@vauxoo.com>
# Copyright (c) 2017, 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017 Mitar <mitar.github@tnode.com>
# Copyright (c) 2018, 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2018 Jim Robertson <jrobertson98atx@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Mitchell T.H. Young <mitchelly@gmail.com>
# Copyright (c) 2018 Adrian Chirieac <chirieacam@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2019 Danny Hermes <daniel.j.hermes@gmail.com>
# Copyright (c) 2019 Zeb Nicholls <zebedee.nicholls@climate-energy-college.org>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 allanc65 <95424144+allanc65@users.noreply.github.com>
# Copyright (c) 2021 Konstantina Saketou <56515303+ksaketou@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Utility methods for docstring checking."""

import re
from typing import List, Set, Tuple

from metaflow._vendor import astroid
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint.checkers import utils


def space_indentation(s):
    """The number of leading spaces in a string

    :param str s: input string

    :rtype: int
    :return: number of leading spaces
    """
    return len(s) - len(s.lstrip(" "))


def get_setters_property_name(node):
    """Get the name of the property that the given node is a setter for.

    :param node: The node to get the property name for.
    :type node: str

    :rtype: str or None
    :returns: The name of the property that the node is a setter for,
        or None if one could not be found.
    """
    decorators = node.decorators.nodes if node.decorators else []
    for decorator in decorators:
        if (
            isinstance(decorator, nodes.Attribute)
            and decorator.attrname == "setter"
            and isinstance(decorator.expr, nodes.Name)
        ):
            return decorator.expr.name
    return None


def get_setters_property(node):
    """Get the property node for the given setter node.

    :param node: The node to get the property for.
    :type node: nodes.FunctionDef

    :rtype: nodes.FunctionDef or None
    :returns: The node relating to the property of the given setter node,
        or None if one could not be found.
    """
    property_ = None

    property_name = get_setters_property_name(node)
    class_node = utils.node_frame_class(node)
    if property_name and class_node:
        class_attrs = class_node.getattr(node.name)
        for attr in class_attrs:
            if utils.decorated_with_property(attr):
                property_ = attr
                break

    return property_


def returns_something(return_node):
    """Check if a return node returns a value other than None.

    :param return_node: The return node to check.
    :type return_node: astroid.Return

    :rtype: bool
    :return: True if the return node returns a value other than None,
        False otherwise.
    """
    returns = return_node.value

    if returns is None:
        return False

    return not (isinstance(returns, nodes.Const) and returns.value is None)


def _get_raise_target(node):
    if isinstance(node.exc, nodes.Call):
        func = node.exc.func
        if isinstance(func, (nodes.Name, nodes.Attribute)):
            return utils.safe_infer(func)
    return None


def _split_multiple_exc_types(target: str) -> List[str]:
    delimiters = r"(\s*,(?:\s*or\s)?\s*|\s+or\s+)"
    return re.split(delimiters, target)


def possible_exc_types(node):
    """
    Gets all of the possible raised exception types for the given raise node.

    .. note::

        Caught exception types are ignored.


    :param node: The raise node to find exception types for.
    :type node: nodes.NodeNG

    :returns: A list of exception types possibly raised by :param:`node`.
    :rtype: set(str)
    """
    excs = []
    if isinstance(node.exc, nodes.Name):
        inferred = utils.safe_infer(node.exc)
        if inferred:
            excs = [inferred.name]
    elif node.exc is None:
        handler = node.parent
        while handler and not isinstance(handler, nodes.ExceptHandler):
            handler = handler.parent

        if handler and handler.type:
            inferred_excs = astroid.unpack_infer(handler.type)
            excs = (exc.name for exc in inferred_excs if exc is not astroid.Uninferable)
    else:
        target = _get_raise_target(node)
        if isinstance(target, nodes.ClassDef):
            excs = [target.name]
        elif isinstance(target, nodes.FunctionDef):
            for ret in target.nodes_of_class(nodes.Return):
                if ret.frame() != target:
                    # return from inner function - ignore it
                    continue

                val = utils.safe_infer(ret.value)
                if (
                    val
                    and isinstance(val, (astroid.Instance, nodes.ClassDef))
                    and utils.inherit_from_std_ex(val)
                ):
                    excs.append(val.name)

    try:
        return {exc for exc in excs if not utils.node_ignores_exception(node, exc)}
    except astroid.InferenceError:
        return set()


def docstringify(docstring, default_type="default"):
    for docstring_type in (
        SphinxDocstring,
        EpytextDocstring,
        GoogleDocstring,
        NumpyDocstring,
    ):
        instance = docstring_type(docstring)
        if instance.is_valid():
            return instance

    docstring_type = DOCSTRING_TYPES.get(default_type, Docstring)
    return docstring_type(docstring)


class Docstring:
    re_for_parameters_see = re.compile(
        r"""
        For\s+the\s+(other)?\s*parameters\s*,\s+see
        """,
        re.X | re.S,
    )

    supports_yields: bool = False
    """True if the docstring supports a "yield" section.

    False if the docstring uses the returns section to document generators.
    """

    # These methods are designed to be overridden
    # pylint: disable=no-self-use
    def __init__(self, doc):
        doc = doc or ""
        self.doc = doc.expandtabs()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}:'''{self.doc}'''>"

    def is_valid(self):
        return False

    def exceptions(self):
        return set()

    def has_params(self):
        return False

    def has_returns(self):
        return False

    def has_rtype(self):
        return False

    def has_property_returns(self):
        return False

    def has_property_type(self):
        return False

    def has_yields(self):
        return False

    def has_yields_type(self):
        return False

    def match_param_docs(self):
        return set(), set()

    def params_documented_elsewhere(self):
        return self.re_for_parameters_see.search(self.doc) is not None


class SphinxDocstring(Docstring):
    re_type = r"""
        [~!.]?               # Optional link style prefix
        \w(?:\w|\.[^\.])*    # Valid python name
        """

    re_simple_container_type = fr"""
        {re_type}                     # a container type
        [\(\[] [^\n\s]+ [\)\]]        # with the contents of the container
    """

    re_multiple_simple_type = r"""
        (?:{container_type}|{type})
        (?:(?:\s+(?:of|or)\s+|\s*,\s*)(?:{container_type}|{type}))*
    """.format(
        type=re_type, container_type=re_simple_container_type
    )

    re_xref = fr"""
        (?::\w+:)?                    # optional tag
        `{re_type}`                   # what to reference
        """

    re_param_raw = fr"""
        :                       # initial colon
        (?:                     # Sphinx keywords
        param|parameter|
        arg|argument|
        key|keyword
        )
        \s+                     # whitespace

        (?:                     # optional type declaration
        ({re_type}|{re_simple_container_type})
        \s+
        )?

        ((\\\*{{1,2}}\w+)|(\w+))  # Parameter name with potential asterisks
        \s*                       # whitespace
        :                         # final colon
        """
    re_param_in_docstring = re.compile(re_param_raw, re.X | re.S)

    re_type_raw = fr"""
        :type                           # Sphinx keyword
        \s+                             # whitespace
        ({re_multiple_simple_type})     # Parameter name
        \s*                             # whitespace
        :                               # final colon
        """
    re_type_in_docstring = re.compile(re_type_raw, re.X | re.S)

    re_property_type_raw = fr"""
        :type:                      # Sphinx keyword
        \s+                         # whitespace
        {re_multiple_simple_type}   # type declaration
        """
    re_property_type_in_docstring = re.compile(re_property_type_raw, re.X | re.S)

    re_raise_raw = fr"""
        :                               # initial colon
        (?:                             # Sphinx keyword
        raises?|
        except|exception
        )
        \s+                             # whitespace
        ({re_multiple_simple_type})     # exception type
        \s*                             # whitespace
        :                               # final colon
        """
    re_raise_in_docstring = re.compile(re_raise_raw, re.X | re.S)

    re_rtype_in_docstring = re.compile(r":rtype:")

    re_returns_in_docstring = re.compile(r":returns?:")

    supports_yields = False

    def is_valid(self):
        return bool(
            self.re_param_in_docstring.search(self.doc)
            or self.re_raise_in_docstring.search(self.doc)
            or self.re_rtype_in_docstring.search(self.doc)
            or self.re_returns_in_docstring.search(self.doc)
            or self.re_property_type_in_docstring.search(self.doc)
        )

    def exceptions(self):
        types = set()

        for match in re.finditer(self.re_raise_in_docstring, self.doc):
            raise_type = match.group(1)
            types.update(_split_multiple_exc_types(raise_type))

        return types

    def has_params(self):
        if not self.doc:
            return False

        return self.re_param_in_docstring.search(self.doc) is not None

    def has_returns(self):
        if not self.doc:
            return False

        return bool(self.re_returns_in_docstring.search(self.doc))

    def has_rtype(self):
        if not self.doc:
            return False

        return bool(self.re_rtype_in_docstring.search(self.doc))

    def has_property_returns(self):
        if not self.doc:
            return False

        # The summary line is the return doc,
        # so the first line must not be a known directive.
        return not self.doc.lstrip().startswith(":")

    def has_property_type(self):
        if not self.doc:
            return False

        return bool(self.re_property_type_in_docstring.search(self.doc))

    def match_param_docs(self):
        params_with_doc = set()
        params_with_type = set()

        for match in re.finditer(self.re_param_in_docstring, self.doc):
            name = match.group(2)
            # Remove escape characters necessary for asterisks
            name = name.replace("\\", "")
            params_with_doc.add(name)
            param_type = match.group(1)
            if param_type is not None:
                params_with_type.add(name)

        params_with_type.update(re.findall(self.re_type_in_docstring, self.doc))
        return params_with_doc, params_with_type


class EpytextDocstring(SphinxDocstring):
    """
    Epytext is similar to Sphinx. See the docs:
        http://epydoc.sourceforge.net/epytext.html
        http://epydoc.sourceforge.net/fields.html#fields

    It's used in PyCharm:
        https://www.jetbrains.com/help/pycharm/2016.1/creating-documentation-comments.html#d848203e314
        https://www.jetbrains.com/help/pycharm/2016.1/using-docstrings-to-specify-types.html
    """

    re_param_in_docstring = re.compile(
        SphinxDocstring.re_param_raw.replace(":", "@", 1), re.X | re.S
    )

    re_type_in_docstring = re.compile(
        SphinxDocstring.re_type_raw.replace(":", "@", 1), re.X | re.S
    )

    re_property_type_in_docstring = re.compile(
        SphinxDocstring.re_property_type_raw.replace(":", "@", 1), re.X | re.S
    )

    re_raise_in_docstring = re.compile(
        SphinxDocstring.re_raise_raw.replace(":", "@", 1), re.X | re.S
    )

    re_rtype_in_docstring = re.compile(
        r"""
        @                       # initial "at" symbol
        (?:                     # Epytext keyword
        rtype|returntype
        )
        :                       # final colon
        """,
        re.X | re.S,
    )

    re_returns_in_docstring = re.compile(r"@returns?:")

    def has_property_returns(self):
        if not self.doc:
            return False

        # If this is a property docstring, the summary is the return doc.
        if self.has_property_type():
            # The summary line is the return doc,
            # so the first line must not be a known directive.
            return not self.doc.lstrip().startswith("@")

        return False


class GoogleDocstring(Docstring):
    re_type = SphinxDocstring.re_type

    re_xref = SphinxDocstring.re_xref

    re_container_type = fr"""
        (?:{re_type}|{re_xref})       # a container type
        [\(\[] [^\n]+ [\)\]]          # with the contents of the container
    """

    re_multiple_type = r"""
        (?:{container_type}|{type}|{xref})
        (?:(?:\s+(?:of|or)\s+|\s*,\s*)(?:{container_type}|{type}|{xref}))*
    """.format(
        type=re_type, xref=re_xref, container_type=re_container_type
    )

    _re_section_template = r"""
        ^([ ]*)   {0} \s*:   \s*$     # Google parameter header
        (  .* )                       # section
        """

    re_param_section = re.compile(
        _re_section_template.format(r"(?:Args|Arguments|Parameters)"),
        re.X | re.S | re.M,
    )

    re_keyword_param_section = re.compile(
        _re_section_template.format(r"Keyword\s(?:Args|Arguments|Parameters)"),
        re.X | re.S | re.M,
    )

    re_param_line = re.compile(
        fr"""
        \s*  (\*{{0,2}}\w+)             # identifier potentially with asterisks
        \s*  ( [(]
            {re_multiple_type}
            (?:,\s+optional)?
            [)] )? \s* :                # optional type declaration
        \s*  (.*)                       # beginning of optional description
    """,
        re.X | re.S | re.M,
    )

    re_raise_section = re.compile(
        _re_section_template.format(r"Raises"), re.X | re.S | re.M
    )

    re_raise_line = re.compile(
        fr"""
        \s*  ({re_multiple_type}) \s* :  # identifier
        \s*  (.*)                        # beginning of optional description
    """,
        re.X | re.S | re.M,
    )

    re_returns_section = re.compile(
        _re_section_template.format(r"Returns?"), re.X | re.S | re.M
    )

    re_returns_line = re.compile(
        fr"""
        \s* ({re_multiple_type}:)?        # identifier
        \s* (.*)                          # beginning of description
    """,
        re.X | re.S | re.M,
    )

    re_property_returns_line = re.compile(
        fr"""
        ^{re_multiple_type}:           # indentifier
        \s* (.*)                       # Summary line / description
    """,
        re.X | re.S | re.M,
    )

    re_yields_section = re.compile(
        _re_section_template.format(r"Yields?"), re.X | re.S | re.M
    )

    re_yields_line = re_returns_line

    supports_yields = True

    def is_valid(self):
        return bool(
            self.re_param_section.search(self.doc)
            or self.re_raise_section.search(self.doc)
            or self.re_returns_section.search(self.doc)
            or self.re_yields_section.search(self.doc)
            or self.re_property_returns_line.search(self._first_line())
        )

    def has_params(self):
        if not self.doc:
            return False

        return self.re_param_section.search(self.doc) is not None

    def has_returns(self):
        if not self.doc:
            return False

        entries = self._parse_section(self.re_returns_section)
        for entry in entries:
            match = self.re_returns_line.match(entry)
            if not match:
                continue

            return_desc = match.group(2)
            if return_desc:
                return True

        return False

    def has_rtype(self):
        if not self.doc:
            return False

        entries = self._parse_section(self.re_returns_section)
        for entry in entries:
            match = self.re_returns_line.match(entry)
            if not match:
                continue

            return_type = match.group(1)
            if return_type:
                return True

        return False

    def has_property_returns(self):
        # The summary line is the return doc,
        # so the first line must not be a known directive.
        first_line = self._first_line()
        return not bool(
            self.re_param_section.search(first_line)
            or self.re_raise_section.search(first_line)
            or self.re_returns_section.search(first_line)
            or self.re_yields_section.search(first_line)
        )

    def has_property_type(self):
        if not self.doc:
            return False

        return bool(self.re_property_returns_line.match(self._first_line()))

    def has_yields(self):
        if not self.doc:
            return False

        entries = self._parse_section(self.re_yields_section)
        for entry in entries:
            match = self.re_yields_line.match(entry)
            if not match:
                continue

            yield_desc = match.group(2)
            if yield_desc:
                return True

        return False

    def has_yields_type(self):
        if not self.doc:
            return False

        entries = self._parse_section(self.re_yields_section)
        for entry in entries:
            match = self.re_yields_line.match(entry)
            if not match:
                continue

            yield_type = match.group(1)
            if yield_type:
                return True

        return False

    def exceptions(self):
        types = set()

        entries = self._parse_section(self.re_raise_section)
        for entry in entries:
            match = self.re_raise_line.match(entry)
            if not match:
                continue

            exc_type = match.group(1)
            exc_desc = match.group(2)
            if exc_desc:
                types.update(_split_multiple_exc_types(exc_type))

        return types

    def match_param_docs(self):
        params_with_doc = set()
        params_with_type = set()

        entries = self._parse_section(self.re_param_section)
        entries.extend(self._parse_section(self.re_keyword_param_section))
        for entry in entries:
            match = self.re_param_line.match(entry)
            if not match:
                continue

            param_name = match.group(1)
            param_type = match.group(2)
            param_desc = match.group(3)

            if param_type:
                params_with_type.add(param_name)

            if param_desc:
                params_with_doc.add(param_name)

        return params_with_doc, params_with_type

    def _first_line(self):
        return self.doc.lstrip().split("\n", 1)[0]

    @staticmethod
    def min_section_indent(section_match):
        return len(section_match.group(1)) + 1

    @staticmethod
    def _is_section_header(_):
        # Google parsing does not need to detect section headers,
        # because it works off of indentation level only
        return False

    def _parse_section(self, section_re):
        section_match = section_re.search(self.doc)
        if section_match is None:
            return []

        min_indentation = self.min_section_indent(section_match)

        entries = []
        entry = []
        is_first = True
        for line in section_match.group(2).splitlines():
            if not line.strip():
                continue
            indentation = space_indentation(line)
            if indentation < min_indentation:
                break

            # The first line after the header defines the minimum
            # indentation.
            if is_first:
                min_indentation = indentation
                is_first = False

            if indentation == min_indentation:
                if self._is_section_header(line):
                    break
                # Lines with minimum indentation must contain the beginning
                # of a new parameter documentation.
                if entry:
                    entries.append("\n".join(entry))
                    entry = []

            entry.append(line)

        if entry:
            entries.append("\n".join(entry))

        return entries


class NumpyDocstring(GoogleDocstring):
    _re_section_template = r"""
        ^([ ]*)   {0}   \s*?$          # Numpy parameters header
        \s*     [-=]+   \s*?$          # underline
        (  .* )                        # section
    """

    re_param_section = re.compile(
        _re_section_template.format(r"(?:Args|Arguments|Parameters)"),
        re.X | re.S | re.M,
    )

    re_param_line = re.compile(
        fr"""
        \s*  (\*{{0,2}}\w+)(\s?(:|\n))                                      # identifier with potential asterisks
        \s*  (?:({GoogleDocstring.re_multiple_type})(?:,\s+optional)?\n)?   # optional type declaration
        \s* (.*)                                                            # optional description
    """,
        re.X | re.S,
    )

    re_raise_section = re.compile(
        _re_section_template.format(r"Raises"), re.X | re.S | re.M
    )

    re_raise_line = re.compile(
        fr"""
        \s* ({GoogleDocstring.re_type})$   # type declaration
        \s* (.*)                           # optional description
    """,
        re.X | re.S | re.M,
    )

    re_returns_section = re.compile(
        _re_section_template.format(r"Returns?"), re.X | re.S | re.M
    )

    re_returns_line = re.compile(
        fr"""
        \s* (?:\w+\s+:\s+)? # optional name
        ({GoogleDocstring.re_multiple_type})$   # type declaration
        \s* (.*)                                # optional description
    """,
        re.X | re.S | re.M,
    )

    re_yields_section = re.compile(
        _re_section_template.format(r"Yields?"), re.X | re.S | re.M
    )

    re_yields_line = re_returns_line

    supports_yields = True

    def match_param_docs(self) -> Tuple[Set[str], Set[str]]:
        """Matches parameter documentation section to parameter documentation rules"""
        params_with_doc = set()
        params_with_type = set()

        entries = self._parse_section(self.re_param_section)
        entries.extend(self._parse_section(self.re_keyword_param_section))
        for entry in entries:
            match = self.re_param_line.match(entry)
            if not match:
                continue

            # check if parameter has description only
            re_only_desc = re.match(r"\s*  (\*{0,2}\w+)\s*:?\n", entry)
            if re_only_desc:
                param_name = match.group(1)
                param_desc = match.group(2)
                param_type = None
            else:
                param_name = match.group(1)
                param_type = match.group(3)
                param_desc = match.group(4)

            if param_type:
                params_with_type.add(param_name)

            if param_desc:
                params_with_doc.add(param_name)

        return params_with_doc, params_with_type

    @staticmethod
    def min_section_indent(section_match):
        return len(section_match.group(1))

    @staticmethod
    def _is_section_header(line):
        return bool(re.match(r"\s*-+$", line))


DOCSTRING_TYPES = {
    "sphinx": SphinxDocstring,
    "epytext": EpytextDocstring,
    "google": GoogleDocstring,
    "numpy": NumpyDocstring,
    "default": Docstring,
}
"""A map of the name of the docstring type to its class.

:type: dict(str, type)
"""
