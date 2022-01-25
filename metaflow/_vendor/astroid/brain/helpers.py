from metaflow._vendor.astroid.nodes.scoped_nodes import Module


def register_module_extender(manager, module_name, get_extension_mod):
    def transform(node):
        extension_module = get_extension_mod()
        for name, objs in extension_module.locals.items():
            node.locals[name] = objs
            for obj in objs:
                if obj.parent is extension_module:
                    obj.parent = node

    manager.register_transform(Module, transform, lambda n: n.name == module_name)
