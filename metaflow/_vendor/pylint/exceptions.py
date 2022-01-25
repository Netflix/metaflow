# Copyright (c) 2016-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2016 Glenn Matthews <glenn@e-dad.net>
# Copyright (c) 2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2019 Thomas Hisch <t.hisch@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Exception classes raised by various operations within pylint."""


class InvalidMessageError(Exception):
    """raised when a message creation, registration or addition is rejected"""


class UnknownMessageError(Exception):
    """raised when an unregistered message id is encountered"""


class EmptyReportError(Exception):
    """raised when a report is empty and so should not be displayed"""


class InvalidReporterError(Exception):
    """raised when selected reporter is invalid (e.g. not found)"""


class InvalidArgsError(ValueError):
    """raised when passed arguments are invalid, e.g., have the wrong length"""


class NoLineSuppliedError(Exception):
    """raised when trying to disable a message on a next line without supplying a line number"""
