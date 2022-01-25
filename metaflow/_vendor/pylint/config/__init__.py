# Copyright (c) 2006-2010, 2012-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2008 pyves@crater.logilab.fr <pyves@crater.logilab.fr>
# Copyright (c) 2013 Google, Inc.
# Copyright (c) 2013 John McGehee <jmcgehee@altera.com>
# Copyright (c) 2014-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015 Aru Sahni <arusahni@gmail.com>
# Copyright (c) 2015 John Kirkham <jakirkham@gmail.com>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Erik <erik.eriksson@yahoo.com>
# Copyright (c) 2016 Alexander Todorov <atodorov@otb.bg>
# Copyright (c) 2016 Moises Lopez <moylop260@vauxoo.com>
# Copyright (c) 2017, 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017-2019 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2017 ahirnish <ahirnish@gmail.com>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2018, 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2018 Jim Robertson <jrobertson98atx@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 Gary Tyler McLeod <mail@garytyler.com>
# Copyright (c) 2018 Konstantin <Github@pheanex.de>
# Copyright (c) 2018 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019, 2021 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2019 Janne Rönkkö <jannero@users.noreply.github.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Eisuke Kawashima <e-kwsm@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import os
import pathlib
import pickle
import sys
from datetime import datetime

from metaflow._vendor.pylint.config.configuration_mixin import ConfigurationMixIn
from metaflow._vendor.pylint.config.find_default_config_files import (
    find_default_config_files,
    find_pylintrc,
)
from metaflow._vendor.pylint.config.man_help_formatter import _ManHelpFormatter
from metaflow._vendor.pylint.config.option import Option
from metaflow._vendor.pylint.config.option_manager_mixin import OptionsManagerMixIn
from metaflow._vendor.pylint.config.option_parser import OptionParser
from metaflow._vendor.pylint.config.options_provider_mixin import OptionsProviderMixIn, UnsupportedAction
from metaflow._vendor.pylint.constants import DEFAULT_PYLINT_HOME, OLD_DEFAULT_PYLINT_HOME
from metaflow._vendor.pylint.utils import LinterStats

__all__ = [
    "ConfigurationMixIn",
    "find_default_config_files",
    "_ManHelpFormatter",
    "Option",
    "OptionsManagerMixIn",
    "OptionParser",
    "OptionsProviderMixIn",
    "UnsupportedAction",
]

USER_HOME = os.path.expanduser("~")
if "PYLINTHOME" in os.environ:
    PYLINT_HOME = os.environ["PYLINTHOME"]
    if USER_HOME == "~":
        USER_HOME = os.path.dirname(PYLINT_HOME)
elif USER_HOME == "~":
    PYLINT_HOME = OLD_DEFAULT_PYLINT_HOME
else:
    PYLINT_HOME = DEFAULT_PYLINT_HOME
    # The spam prevention is due to pylint being used in parallel by
    # pre-commit, and the message being spammy in this context
    # Also if you work with old version of pylint that recreate the
    # old pylint home, you can get the old message for a long time.
    prefix_spam_prevention = "pylint_warned_about_old_cache_already"
    spam_prevention_file = os.path.join(
        PYLINT_HOME,
        datetime.now().strftime(prefix_spam_prevention + "_%Y-%m-%d.temp"),
    )
    old_home = os.path.join(USER_HOME, OLD_DEFAULT_PYLINT_HOME)
    if os.path.exists(old_home) and not os.path.exists(spam_prevention_file):
        print(
            f"PYLINTHOME is now '{PYLINT_HOME}' but obsolescent '{old_home}' is found; "
            "you can safely remove the latter",
            file=sys.stderr,
        )
        # Remove old spam prevention file
        if os.path.exists(PYLINT_HOME):
            for filename in os.listdir(PYLINT_HOME):
                if prefix_spam_prevention in filename:
                    try:
                        os.remove(os.path.join(PYLINT_HOME, filename))
                    except OSError:
                        pass

        # Create spam prevention file for today
        try:
            pathlib.Path(PYLINT_HOME).mkdir(parents=True, exist_ok=True)
            with open(spam_prevention_file, "w", encoding="utf8") as f:
                f.write("")
        except Exception:  # pylint: disable=broad-except
            # Can't write in PYLINT_HOME ?
            print(
                "Can't write the file that was supposed to "
                f"prevent pylint.d deprecation spam in {PYLINT_HOME}."
            )


def _get_pdata_path(base_name, recurs):
    base_name = base_name.replace(os.sep, "_")
    return os.path.join(PYLINT_HOME, f"{base_name}{recurs}.stats")


def load_results(base):
    data_file = _get_pdata_path(base, 1)
    try:
        with open(data_file, "rb") as stream:
            data = pickle.load(stream)
            if not isinstance(data, LinterStats):
                raise TypeError
            return data
    except Exception:  # pylint: disable=broad-except
        return None


def save_results(results, base):
    if not os.path.exists(PYLINT_HOME):
        try:
            os.makedirs(PYLINT_HOME)
        except OSError:
            print(f"Unable to create directory {PYLINT_HOME}", file=sys.stderr)
    data_file = _get_pdata_path(base, 1)
    try:
        with open(data_file, "wb") as stream:
            pickle.dump(results, stream)
    except OSError as ex:
        print(f"Unable to create file {data_file}: {ex}", file=sys.stderr)


PYLINTRC = find_pylintrc()
