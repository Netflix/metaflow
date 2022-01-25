# Copyright (c) 2015-2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2016 Ceridwen <ceridwenv@gmail.com>
# Copyright (c) 2017 Roy Wright <roy@wright.org>
# Copyright (c) 2018 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2019 Antoine Boellinger <aboellinger@hotmail.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Astroid hooks for the PyQT library."""

from metaflow._vendor.astroid import nodes, parse
from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import AstroidBuilder
from metaflow._vendor.astroid.manager import AstroidManager


def _looks_like_signal(node, signal_name="pyqtSignal"):
    if "__class__" in node.instance_attrs:
        try:
            cls = node.instance_attrs["__class__"][0]
            return cls.name == signal_name
        except AttributeError:
            # return False if the cls does not have a name attribute
            pass
    return False


def transform_pyqt_signal(node):
    module = parse(
        """
    class pyqtSignal(object):
        def connect(self, slot, type=None, no_receiver_check=False):
            pass
        def disconnect(self, slot):
            pass
        def emit(self, *args):
            pass
    """
    )
    signal_cls = module["pyqtSignal"]
    node.instance_attrs["emit"] = signal_cls["emit"]
    node.instance_attrs["disconnect"] = signal_cls["disconnect"]
    node.instance_attrs["connect"] = signal_cls["connect"]


def transform_pyside_signal(node):
    module = parse(
        """
    class NotPySideSignal(object):
        def connect(self, receiver, type=None):
            pass
        def disconnect(self, receiver):
            pass
        def emit(self, *args):
            pass
    """
    )
    signal_cls = module["NotPySideSignal"]
    node.instance_attrs["connect"] = signal_cls["connect"]
    node.instance_attrs["disconnect"] = signal_cls["disconnect"]
    node.instance_attrs["emit"] = signal_cls["emit"]


def pyqt4_qtcore_transform():
    return AstroidBuilder(AstroidManager()).string_build(
        """

def SIGNAL(signal_name): pass

class QObject(object):
    def emit(self, signal): pass
"""
    )


register_module_extender(AstroidManager(), "PyQt4.QtCore", pyqt4_qtcore_transform)
AstroidManager().register_transform(
    nodes.FunctionDef, transform_pyqt_signal, _looks_like_signal
)
AstroidManager().register_transform(
    nodes.ClassDef,
    transform_pyside_signal,
    lambda node: node.qname() in {"PySide.QtCore.Signal", "PySide2.QtCore.Signal"},
)
