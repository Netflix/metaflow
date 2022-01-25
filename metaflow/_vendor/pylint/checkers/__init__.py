# Copyright (c) 2006-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2013-2014 Google, Inc.
# Copyright (c) 2013 buck@yelp.com <buck@yelp.com>
# Copyright (c) 2014-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Moises Lopez <moylop260@vauxoo.com>
# Copyright (c) 2017-2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2018-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2019 Bruno P. Kinoshita <kinow@users.noreply.github.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Frank Harrison <frank@doublethefish.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Matus Valo <matusvalo@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""utilities methods and classes for checkers

Base id of standard checkers (used in msg and report ids):
01: base
02: classes
03: format
04: import
05: misc
06: variables
07: exceptions
08: similar
09: design_analysis
10: newstyle
11: typecheck
12: logging
13: string_format
14: string_constant
15: stdlib
16: python3
17: refactoring
18-50: not yet used: reserved for future internal checkers.
51-99: perhaps used: reserved for external checkers

The raw_metrics checker has no number associated since it doesn't emit any
messages nor reports. XXX not true, emit a 07 report !

"""

import sys
from typing import List, Optional, Tuple, Union

from metaflow._vendor.pylint.checkers.base_checker import BaseChecker, BaseTokenChecker
from metaflow._vendor.pylint.checkers.deprecated import DeprecatedMixin
from metaflow._vendor.pylint.checkers.mapreduce_checker import MapReduceMixin
from metaflow._vendor.pylint.utils import LinterStats, diff_string, register_plugins

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from metaflow._vendor.typing_extensions import Literal


def table_lines_from_stats(
    stats: LinterStats,
    old_stats: Optional[LinterStats],
    stat_type: Literal["duplicated_lines", "message_types"],
) -> List[str]:
    """get values listed in <columns> from <stats> and <old_stats>,
    and return a formatted list of values, designed to be given to a
    ureport.Table object
    """
    lines: List[str] = []
    if stat_type == "duplicated_lines":
        new: List[Tuple[str, Union[str, int, float]]] = [
            ("nb_duplicated_lines", stats.duplicated_lines["nb_duplicated_lines"]),
            (
                "percent_duplicated_lines",
                stats.duplicated_lines["percent_duplicated_lines"],
            ),
        ]
        if old_stats:
            old: List[Tuple[str, Union[str, int, float]]] = [
                (
                    "nb_duplicated_lines",
                    old_stats.duplicated_lines["nb_duplicated_lines"],
                ),
                (
                    "percent_duplicated_lines",
                    old_stats.duplicated_lines["percent_duplicated_lines"],
                ),
            ]
        else:
            old = [("nb_duplicated_lines", "NC"), ("percent_duplicated_lines", "NC")]
    elif stat_type == "message_types":
        new = [
            ("convention", stats.convention),
            ("refactor", stats.refactor),
            ("warning", stats.warning),
            ("error", stats.error),
        ]
        if old_stats:
            old = [
                ("convention", old_stats.convention),
                ("refactor", old_stats.refactor),
                ("warning", old_stats.warning),
                ("error", old_stats.error),
            ]
        else:
            old = [
                ("convention", "NC"),
                ("refactor", "NC"),
                ("warning", "NC"),
                ("error", "NC"),
            ]

    for index, _ in enumerate(new):
        new_value = new[index][1]
        old_value = old[index][1]
        diff_str = (
            diff_string(old_value, new_value)
            if isinstance(old_value, float)
            else old_value
        )
        new_str = f"{new_value:.3f}" if isinstance(new_value, float) else str(new_value)
        old_str = f"{old_value:.3f}" if isinstance(old_value, float) else str(old_value)
        lines.extend((new[index][0].replace("_", " "), new_str, old_str, diff_str))
    return lines


def initialize(linter):
    """initialize linter with checkers in this package"""
    register_plugins(linter, __path__[0])


__all__ = [
    "BaseChecker",
    "BaseTokenChecker",
    "initialize",
    "MapReduceMixin",
    "DeprecatedMixin",
    "register_plugins",
]
