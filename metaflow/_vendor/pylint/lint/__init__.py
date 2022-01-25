# Copyright (c) 2006-2015 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2008 Fabrice Douchant <Fabrice.Douchant@logilab.fr>
# Copyright (c) 2009 Vincent
# Copyright (c) 2009 Mads Kiilerich <mads@kiilerich.com>
# Copyright (c) 2011-2014 Google, Inc.
# Copyright (c) 2012 David Pursehouse <david.pursehouse@sonymobile.com>
# Copyright (c) 2012 Kevin Jing Qiu <kevin.jing.qiu@gmail.com>
# Copyright (c) 2012 FELD Boris <lothiraldan@gmail.com>
# Copyright (c) 2012 JT Olds <jtolds@xnet5.com>
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014-2015 Michal Nowikowski <godfryd@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Alexandru Coman <fcoman@bitdefender.com>
# Copyright (c) 2014 Daniel Harding <dharding@living180.net>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2014 Dan Goldsmith <djgoldsmith@googlemail.com>
# Copyright (c) 2015-2016 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2015 Aru Sahni <arusahni@gmail.com>
# Copyright (c) 2015 Steven Myint <hg@stevenmyint.com>
# Copyright (c) 2015 Simu Toni <simutoni@gmail.com>
# Copyright (c) 2015 Mihai Balint <balint.mihai@gmail.com>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016-2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2016 Glenn Matthews <glenn@e-dad.net>
# Copyright (c) 2016 Alan Evangelista <alanoe@linux.vnet.ibm.com>
# Copyright (c) 2017-2019 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017-2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2017 Daniel Miller <millerdev@gmail.com>
# Copyright (c) 2017 Roman Ivanov <me@roivanov.com>
# Copyright (c) 2017 Ned Batchelder <ned@nedbatchelder.com>
# Copyright (c) 2018-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2018, 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2018-2019 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018 Matus Valo <matusvalo@users.noreply.github.com>
# Copyright (c) 2018 Lucas Cimon <lucas.cimon@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Randall Leeds <randall@bleeds.info>
# Copyright (c) 2018 Mike Frysinger <vapier@gmail.com>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 Jason Owen <jason.a.owen@gmail.com>
# Copyright (c) 2018 Gary Tyler McLeod <mail@garytyler.com>
# Copyright (c) 2018 Yuval Langer <yuvallanger@mail.tau.ac.il>
# Copyright (c) 2018 kapsh <kapsh@kap.sh>
# Copyright (c) 2019 syutbai <syutbai@gmail.com>
# Copyright (c) 2019 Thomas Hisch <t.hisch@gmail.com>
# Copyright (c) 2019 Hugues <hugues.bruant@affirm.com>
# Copyright (c) 2019 Janne Rönkkö <jannero@users.noreply.github.com>
# Copyright (c) 2019 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2019 Trevor Bekolay <tbekolay@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2019 Robert Schweizer <robert_schweizer@gmx.de>
# Copyright (c) 2019 Andres Perez Hortal <andresperezcba@gmail.com>
# Copyright (c) 2019 Peter Kolbus <peter.kolbus@gmail.com>
# Copyright (c) 2019 Nicolas Dickreuter <dickreuter@gmail.com>
# Copyright (c) 2020 Frank Harrison <frank@doublethefish.com>
# Copyright (c) 2020 anubh-v <anubhav@u.nus.edu>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

""" pylint [options] modules_or_packages

  Check that module(s) satisfy a coding standard (and more !).

    pylint --help

  Display this help message and exit.

    pylint --help-msg <msg-id>[,<msg-id>]

  Display help messages about given message identifiers and exit.
"""
import sys

from metaflow._vendor.pylint.lint.parallel import check_parallel
from metaflow._vendor.pylint.lint.pylinter import PyLinter
from metaflow._vendor.pylint.lint.report_functions import (
    report_messages_by_module_stats,
    report_messages_stats,
    report_total_messages_stats,
)
from metaflow._vendor.pylint.lint.run import Run
from metaflow._vendor.pylint.lint.utils import (
    ArgumentPreprocessingError,
    _patch_sys_path,
    fix_import_path,
    preprocess_options,
)

__all__ = [
    "check_parallel",
    "PyLinter",
    "report_messages_by_module_stats",
    "report_messages_stats",
    "report_total_messages_stats",
    "Run",
    "ArgumentPreprocessingError",
    "_patch_sys_path",
    "fix_import_path",
    "preprocess_options",
]

if __name__ == "__main__":
    Run(sys.argv[1:])
