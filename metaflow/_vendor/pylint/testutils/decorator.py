# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import functools
import optparse  # pylint: disable=deprecated-module

from metaflow._vendor.pylint.lint import PyLinter
from metaflow._vendor.pylint.testutils.checker_test_case import CheckerTestCase


def set_config(**kwargs):
    """Decorator for setting config values on a checker.

    Passing the args and kwargs back to the test function itself
    allows this decorator to be used on parametrized test cases.
    """

    def _wrapper(fun):
        @functools.wraps(fun)
        def _forward(self, *args, **test_function_kwargs):
            try:
                for key, value in kwargs.items():
                    self.checker.set_option(key.replace("_", "-"), value)
            except optparse.OptionError:
                # Check if option is one of the base options of the PyLinter class
                for key, value in kwargs.items():
                    try:
                        self.checker.set_option(
                            key.replace("_", "-"),
                            value,
                            optdict=dict(PyLinter.make_options())[
                                key.replace("_", "-")
                            ],
                        )
                    except KeyError:
                        # pylint: disable-next=fixme
                        # TODO: Find good way to double load checkers in unittests
                        # When options are used by multiple checkers we need to load both of them
                        # to be able to get an optdict
                        self.checker.set_option(
                            key.replace("_", "-"),
                            value,
                            optdict={},
                        )
            if isinstance(self, CheckerTestCase):
                # reopen checker in case, it may be interested in configuration change
                self.checker.open()
            fun(self, *args, **test_function_kwargs)

        return _forward

    return _wrapper


def set_config_directly(**kwargs):
    """Decorator for setting config values on a checker without validation.

    Some options should be declared in two different checkers. This is
    impossible without duplicating the option key. For example:
    "no-docstring-rgx" in DocstringParameterChecker & DocStringChecker
    This decorator allows to directly set such options.

    Passing the args and kwargs back to the test function itself
    allows this decorator to be used on parametrized test cases.
    """

    def _wrapper(fun):
        @functools.wraps(fun)
        def _forward(self, *args, **test_function_kwargs):
            for key, value in kwargs.items():
                setattr(self.checker.config, key, value)
            if isinstance(self, CheckerTestCase):
                # reopen checker in case, it may be interested in configuration change
                self.checker.open()
            fun(self, *args, **test_function_kwargs)

        return _forward

    return _wrapper
