# Copyright (c) 2017-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2020-2021 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Astroid hooks for the UUID module."""
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes.node_classes import Const
from metaflow._vendor.astroid.nodes.scoped_nodes import ClassDef


def _patch_uuid_class(node):
    # The .int member is patched using __dict__
    node.locals["int"] = [Const(0, parent=node)]


AstroidManager().register_transform(
    ClassDef, _patch_uuid_class, lambda node: node.qname() == "uuid.UUID"
)
