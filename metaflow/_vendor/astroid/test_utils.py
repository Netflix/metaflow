# Copyright (c) 2013-2014 Google, Inc.
# Copyright (c) 2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2015-2016, 2018-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015-2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2016 Jakub Wilk <jwilk@jwilk.net>
# Copyright (c) 2018 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Andrew Haigh <hello@nelf.in>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Utility functions for test code that uses astroid ASTs as input."""
import contextlib
import functools
import sys
import warnings
from typing import Callable, Tuple

import pytest

from metaflow._vendor.astroid import manager, nodes, transforms


def require_version(minver: str = "0.0.0", maxver: str = "4.0.0") -> Callable:
    """Compare version of python interpreter to the given one.
    Skip the test if older.
    """

    def parse(python_version: str) -> Tuple[int, ...]:
        try:
            return tuple(int(v) for v in python_version.split("."))
        except ValueError as e:
            msg = f"{python_version} is not a correct version : should be X.Y[.Z]."
            raise ValueError(msg) from e

    min_version = parse(minver)
    max_version = parse(maxver)

    def check_require_version(f):
        current: Tuple[int, int, int] = sys.version_info[:3]
        if min_version < current <= max_version:
            return f

        version: str = ".".join(str(v) for v in sys.version_info)

        @functools.wraps(f)
        def new_f(*args, **kwargs):
            if minver != "0.0.0":
                pytest.skip(f"Needs Python > {minver}. Current version is {version}.")
            elif maxver != "4.0.0":
                pytest.skip(f"Needs Python <= {maxver}. Current version is {version}.")

        return new_f

    return check_require_version


def get_name_node(start_from, name, index=0):
    return [n for n in start_from.nodes_of_class(nodes.Name) if n.name == name][index]


@contextlib.contextmanager
def enable_warning(warning):
    warnings.simplefilter("always", warning)
    try:
        yield
    finally:
        # Reset it to default value, so it will take
        # into account the values from the -W flag.
        warnings.simplefilter("default", warning)


def brainless_manager():
    m = manager.AstroidManager()
    # avoid caching into the AstroidManager borg since we get problems
    # with other tests :
    m.__dict__ = {}
    m._failed_import_hooks = []
    m.astroid_cache = {}
    m._mod_file_cache = {}
    m._transform = transforms.TransformVisitor()
    m.extension_package_whitelist = {}
    return m
