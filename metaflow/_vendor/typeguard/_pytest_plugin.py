from __future__ import annotations

import sys
import warnings

from pytest import Config, Parser

from metaflow._vendor.typeguard._config import CollectionCheckStrategy, ForwardRefPolicy, global_config
from metaflow._vendor.typeguard._exceptions import InstrumentationWarning
from metaflow._vendor.typeguard._importhook import install_import_hook
from metaflow._vendor.typeguard._utils import qualified_name, resolve_reference


def pytest_addoption(parser: Parser) -> None:
    group = parser.getgroup("typeguard")
    group.addoption(
        "--typeguard-packages",
        action="store",
        help="comma separated name list of packages and modules to instrument for "
        "type checking, or :all: to instrument all modules loaded after typeguard",
    )
    group.addoption(
        "--typeguard-debug-instrumentation",
        action="store_true",
        help="print all instrumented code to stderr",
    )
    group.addoption(
        "--typeguard-typecheck-fail-callback",
        action="store",
        help=(
            "a module:varname (e.g. typeguard:warn_on_error) reference to a function "
            "that is called (with the exception, and memo object as arguments) to "
            "handle a TypeCheckError"
        ),
    )
    group.addoption(
        "--typeguard-forward-ref-policy",
        action="store",
        choices=list(ForwardRefPolicy.__members__),
        help=(
            "determines how to deal with unresolveable forward references in type "
            "annotations"
        ),
    )
    group.addoption(
        "--typeguard-collection-check-strategy",
        action="store",
        choices=list(CollectionCheckStrategy.__members__),
        help="determines how thoroughly to check collections (list, dict, etc)",
    )


def pytest_configure(config: Config) -> None:
    packages_option = config.getoption("typeguard_packages")
    if packages_option:
        if packages_option == ":all:":
            packages: list[str] | None = None
        else:
            packages = [pkg.strip() for pkg in packages_option.split(",")]
            already_imported_packages = sorted(
                package for package in packages if package in sys.modules
            )
            if already_imported_packages:
                warnings.warn(
                    f"typeguard cannot check these packages because they are already "
                    f"imported: {', '.join(already_imported_packages)}",
                    InstrumentationWarning,
                    stacklevel=1,
                )

        install_import_hook(packages=packages)

    debug_option = config.getoption("typeguard_debug_instrumentation")
    if debug_option:
        global_config.debug_instrumentation = True

    fail_callback_option = config.getoption("typeguard_typecheck_fail_callback")
    if fail_callback_option:
        callback = resolve_reference(fail_callback_option)
        if not callable(callback):
            raise TypeError(
                f"{fail_callback_option} ({qualified_name(callback.__class__)}) is not "
                f"a callable"
            )

        global_config.typecheck_fail_callback = callback

    forward_ref_policy_option = config.getoption("typeguard_forward_ref_policy")
    if forward_ref_policy_option:
        forward_ref_policy = ForwardRefPolicy.__members__[forward_ref_policy_option]
        global_config.forward_ref_policy = forward_ref_policy

    collection_check_strategy_option = config.getoption(
        "typeguard_collection_check_strategy"
    )
    if collection_check_strategy_option:
        collection_check_strategy = CollectionCheckStrategy.__members__[
            collection_check_strategy_option
        ]
        global_config.collection_check_strategy = collection_check_strategy
