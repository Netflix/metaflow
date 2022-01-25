# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import copy
import optparse  # pylint: disable=deprecated-module
import pathlib
import re
from typing import List, Pattern, Union

from metaflow._vendor.pylint import utils


# pylint: disable=unused-argument
def _csv_validator(_, name, value):
    return utils._check_csv(value)


# pylint: disable=unused-argument
def _regexp_validator(_, name, value):
    if hasattr(value, "pattern"):
        return value
    return re.compile(value)


# pylint: disable=unused-argument
def _regexp_csv_validator(_, name, value):
    return [_regexp_validator(_, name, val) for val in _csv_validator(_, name, value)]


def _regexp_paths_csv_validator(
    _, name: str, value: Union[str, List[Pattern[str]]]
) -> List[Pattern[str]]:
    if isinstance(value, list):
        return value
    patterns = []
    for val in _csv_validator(_, name, value):
        patterns.append(
            re.compile(
                str(pathlib.PureWindowsPath(val)).replace("\\", "\\\\")
                + "|"
                + pathlib.PureWindowsPath(val).as_posix()
            )
        )
    return patterns


def _choice_validator(choices, name, value):
    if value not in choices:
        msg = "option %s: invalid value: %r, should be in %s"
        raise optparse.OptionValueError(msg % (name, value, choices))
    return value


def _yn_validator(opt, _, value):
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        value = value.lower()
    if value in {"y", "yes", "true"}:
        return True
    if value in {"n", "no", "false"}:
        return False
    msg = "option %s: invalid yn value %r, should be in (y, yes, true, n, no, false)"
    raise optparse.OptionValueError(msg % (opt, value))


def _multiple_choice_validator(choices, name, value):
    values = utils._check_csv(value)
    for csv_value in values:
        if csv_value not in choices:
            msg = "option %s: invalid value: %r, should be in %s"
            raise optparse.OptionValueError(msg % (name, csv_value, choices))
    return values


def _non_empty_string_validator(opt, _, value):
    if not value:
        msg = "indent string can't be empty."
        raise optparse.OptionValueError(msg)
    return utils._unquote(value)


def _multiple_choices_validating_option(opt, name, value):
    return _multiple_choice_validator(opt.choices, name, value)


def _py_version_validator(_, name, value):
    if not isinstance(value, tuple):
        try:
            value = tuple(int(val) for val in value.split("."))
        except (ValueError, AttributeError):
            raise optparse.OptionValueError(
                f"Invalid format for {name}, should be version string. E.g., '3.8'"
            ) from None
    return value


VALIDATORS = {
    "string": utils._unquote,
    "int": int,
    "float": float,
    "regexp": lambda pattern: re.compile(pattern or ""),
    "regexp_csv": _regexp_csv_validator,
    "regexp_paths_csv": _regexp_paths_csv_validator,
    "csv": _csv_validator,
    "yn": _yn_validator,
    "choice": lambda opt, name, value: _choice_validator(opt["choices"], name, value),
    "multiple_choice": lambda opt, name, value: _multiple_choice_validator(
        opt["choices"], name, value
    ),
    "non_empty_string": _non_empty_string_validator,
    "py_version": _py_version_validator,
}


def _call_validator(opttype, optdict, option, value):
    if opttype not in VALIDATORS:
        raise Exception(f'Unsupported type "{opttype}"')
    try:
        return VALIDATORS[opttype](optdict, option, value)
    except TypeError:
        try:
            return VALIDATORS[opttype](value)
        except Exception as e:
            raise optparse.OptionValueError(
                f"{option} value ({value!r}) should be of type {opttype}"
            ) from e


def _validate(value, optdict, name=""):
    """return a validated value for an option according to its type

    optional argument name is only used for error message formatting
    """
    try:
        _type = optdict["type"]
    except KeyError:
        return value
    return _call_validator(_type, optdict, name, value)


# pylint: disable=no-member
class Option(optparse.Option):
    TYPES = optparse.Option.TYPES + (
        "regexp",
        "regexp_csv",
        "regexp_paths_csv",
        "csv",
        "yn",
        "multiple_choice",
        "non_empty_string",
        "py_version",
    )
    ATTRS = optparse.Option.ATTRS + ["hide", "level"]
    TYPE_CHECKER = copy.copy(optparse.Option.TYPE_CHECKER)
    TYPE_CHECKER["regexp"] = _regexp_validator
    TYPE_CHECKER["regexp_csv"] = _regexp_csv_validator
    TYPE_CHECKER["regexp_paths_csv"] = _regexp_paths_csv_validator
    TYPE_CHECKER["csv"] = _csv_validator
    TYPE_CHECKER["yn"] = _yn_validator
    TYPE_CHECKER["multiple_choice"] = _multiple_choices_validating_option
    TYPE_CHECKER["non_empty_string"] = _non_empty_string_validator
    TYPE_CHECKER["py_version"] = _py_version_validator

    def __init__(self, *opts, **attrs):
        super().__init__(*opts, **attrs)
        if hasattr(self, "hide") and self.hide:
            self.help = optparse.SUPPRESS_HELP

    def _check_choice(self):
        if self.type in {"choice", "multiple_choice"}:
            if self.choices is None:
                raise optparse.OptionError(
                    "must supply a list of choices for type 'choice'", self
                )
            if not isinstance(self.choices, (tuple, list)):
                raise optparse.OptionError(
                    # pylint: disable-next=consider-using-f-string
                    "choices must be a list of strings ('%s' supplied)"
                    % str(type(self.choices)).split("'")[1],
                    self,
                )
        elif self.choices is not None:
            raise optparse.OptionError(
                f"must not supply choices for type {self.type!r}", self
            )

    optparse.Option.CHECK_METHODS[2] = _check_choice  # type: ignore[index]

    def process(self, opt, value, values, parser):
        # First, convert the value(s) to the right type.  Howl if any
        # value(s) are bogus.
        value = self.convert_value(opt, value)
        if self.type == "named":
            existent = getattr(values, self.dest)
            if existent:
                existent.update(value)
                value = existent
        # And then take whatever action is expected of us.
        # This is a separate method to make life easier for
        # subclasses to add new actions.
        return self.take_action(self.action, self.dest, opt, value, values, parser)
