# Copyright (c) 2007, 2010, 2013, 2015 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2013 Google, Inc.
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015 Mike Frysinger <vapier@gentoo.org>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Glenn Matthews <glenn@e-dad.net>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 谭九鼎 <109224573@qq.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 bot <bot@noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import sys
import tokenize
from typing import Any, Optional, cast

from metaflow._vendor.pylint.checkers import BaseTokenChecker
from metaflow._vendor.pylint.interfaces import ITokenChecker
from metaflow._vendor.pylint.reporters.ureports.nodes import Table
from metaflow._vendor.pylint.utils import LinterStats, diff_string

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from metaflow._vendor.typing_extensions import Literal


def report_raw_stats(
    sect,
    stats: LinterStats,
    old_stats: Optional[LinterStats],
) -> None:
    """calculate percentage of code / doc / comment / empty"""
    total_lines = stats.code_type_count["total"]
    sect.description = f"{total_lines} lines have been analyzed"
    lines = ["type", "number", "%", "previous", "difference"]
    for node_type in ("code", "docstring", "comment", "empty"):
        node_type = cast(Literal["code", "docstring", "comment", "empty"], node_type)
        total = stats.code_type_count[node_type]
        percent = float(total * 100) / total_lines if total_lines else None
        old = old_stats.code_type_count[node_type] if old_stats else None
        diff_str = diff_string(old, total) if old else None
        lines += [
            node_type,
            str(total),
            f"{percent:.2f}" if percent is not None else "NC",
            str(old) if old else "NC",
            diff_str if diff_str else "NC",
        ]
    sect.append(Table(children=lines, cols=5, rheaders=1))


class RawMetricsChecker(BaseTokenChecker):
    """does not check anything but gives some raw metrics :
    * total number of lines
    * total number of code lines
    * total number of docstring lines
    * total number of comments lines
    * total number of empty lines
    """

    __implements__ = (ITokenChecker,)

    # configuration section name
    name = "metrics"
    # configuration options
    options = ()
    # messages
    msgs: Any = {}
    # reports
    reports = (("RP0701", "Raw metrics", report_raw_stats),)

    def __init__(self, linter):
        super().__init__(linter)

    def open(self):
        """init statistics"""
        self.linter.stats.reset_code_count()

    def process_tokens(self, tokens):
        """update stats"""
        i = 0
        tokens = list(tokens)
        while i < len(tokens):
            i, lines_number, line_type = get_type(tokens, i)
            self.linter.stats.code_type_count["total"] += lines_number
            self.linter.stats.code_type_count[line_type] += lines_number


JUNK = (tokenize.NL, tokenize.INDENT, tokenize.NEWLINE, tokenize.ENDMARKER)


def get_type(tokens, start_index):
    """return the line type : docstring, comment, code, empty"""
    i = start_index
    start = tokens[i][2]
    pos = start
    line_type = None
    while i < len(tokens) and tokens[i][2][0] == start[0]:
        tok_type = tokens[i][0]
        pos = tokens[i][3]
        if line_type is None:
            if tok_type == tokenize.STRING:
                line_type = "docstring"
            elif tok_type == tokenize.COMMENT:
                line_type = "comment"
            elif tok_type in JUNK:
                pass
            else:
                line_type = "code"
        i += 1
    if line_type is None:
        line_type = "empty"
    elif i < len(tokens) and tokens[i][0] == tokenize.NEWLINE:
        i += 1
    return i, pos[0] - start[0] + 1, line_type


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(RawMetricsChecker(linter))
