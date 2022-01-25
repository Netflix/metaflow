# Copyright (c) 2015-2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015 raylu <lurayl@gmail.com>
# Copyright (c) 2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Astroid hooks for dateutil"""

import textwrap

from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import AstroidBuilder
from metaflow._vendor.astroid.manager import AstroidManager


def dateutil_transform():
    return AstroidBuilder(AstroidManager()).string_build(
        textwrap.dedent(
            """
    import datetime
    def parse(timestr, parserinfo=None, **kwargs):
        return datetime.datetime()
    """
        )
    )


register_module_extender(AstroidManager(), "dateutil.parser", dateutil_transform)
