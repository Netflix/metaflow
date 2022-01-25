# Copyright (c) 2012-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2012 FELD Boris <lothiraldan@gmail.com>
# Copyright (c) 2013-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2013-2014 Google, Inc.
# Copyright (c) 2013 buck@yelp.com <buck@yelp.com>
# Copyright (c) 2014 LCD 47 <lcd047@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Ricardo Gemignani <ricardo.gemignani@gmail.com>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Pavel Roskin <proski@gnu.org>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Derek Gustafson <degustaf@gmail.com>
# Copyright (c) 2016 Roy Williams <roy.williams.iii@gmail.com>
# Copyright (c) 2016 xmo-odoo <xmo-odoo@users.noreply.github.com>
# Copyright (c) 2017 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019 Mr. Senko <atodorov@mrsenko.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 谭九鼎 <109224573@qq.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Lefteris Karapetsas <lefteris@refu.co>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Functional/non regression tests for pylint"""

__all__ = [
    "_get_tests_info",
    "_tokenize_str",
    "CheckerTestCase",
    "FunctionalTestFile",
    "linter",
    "LintModuleTest",
    "MessageTest",
    "MinimalTestReporter",
    "set_config",
    "GenericTestReporter",
    "UPDATE_FILE",
    "UPDATE_OPTION",
    "UnittestLinter",
]

from metaflow._vendor.pylint.testutils.checker_test_case import CheckerTestCase
from metaflow._vendor.pylint.testutils.constants import UPDATE_FILE, UPDATE_OPTION
from metaflow._vendor.pylint.testutils.decorator import set_config
from metaflow._vendor.pylint.testutils.functional_test_file import FunctionalTestFile
from metaflow._vendor.pylint.testutils.get_test_info import _get_tests_info
from metaflow._vendor.pylint.testutils.global_test_linter import linter
from metaflow._vendor.pylint.testutils.lint_module_test import LintModuleTest
from metaflow._vendor.pylint.testutils.output_line import MessageTest
from metaflow._vendor.pylint.testutils.reporter_for_tests import GenericTestReporter, MinimalTestReporter
from metaflow._vendor.pylint.testutils.tokenize_str import _tokenize_str
from metaflow._vendor.pylint.testutils.unittest_linter import UnittestLinter
