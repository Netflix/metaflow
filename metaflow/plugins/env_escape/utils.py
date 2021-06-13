import inspect


def get_methods(class_object):
    all_attributes = {}
    all_methods = {}
    if isinstance(class_object, type):
        mros = list(reversed(type(class_object).__mro__)) + list(
            reversed(class_object.__mro__)
        )
    else:
        mros = reversed(type(class_object).__mro__)
    for base_class in mros:
        all_attributes.update(base_class.__dict__)
    for name, attribute in all_attributes.items():
        if hasattr(attribute, "__call__"):
            all_methods[name] = inspect.getdoc(attribute)
        elif isinstance(attribute, staticmethod):
            all_methods["___s___%s" % name] = inspect.getdoc(attribute)
        elif isinstance(attribute, classmethod):
            all_methods["___c___%s" % name] = inspect.getdoc(attribute)
    return all_methods
