# Copyright (c) 2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Astroid brain hints for some of the _io C objects."""
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes import ClassDef

BUFFERED = {"BufferedWriter", "BufferedReader"}
TextIOWrapper = "TextIOWrapper"
FileIO = "FileIO"
BufferedWriter = "BufferedWriter"


def _generic_io_transform(node, name, cls):
    """Transform the given name, by adding the given *class* as a member of the node."""

    io_module = AstroidManager().ast_from_module_name("_io")
    attribute_object = io_module[cls]
    instance = attribute_object.instantiate_class()
    node.locals[name] = [instance]


def _transform_text_io_wrapper(node):
    # This is not always correct, since it can vary with the type of the descriptor,
    # being stdout, stderr or stdin. But we cannot get access to the name of the
    # stream, which is why we are using the BufferedWriter class as a default
    # value
    return _generic_io_transform(node, name="buffer", cls=BufferedWriter)


def _transform_buffered(node):
    return _generic_io_transform(node, name="raw", cls=FileIO)


AstroidManager().register_transform(
    ClassDef, _transform_buffered, lambda node: node.name in BUFFERED
)
AstroidManager().register_transform(
    ClassDef, _transform_text_io_wrapper, lambda node: node.name == TextIOWrapper
)
