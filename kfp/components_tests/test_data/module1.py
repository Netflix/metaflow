module_level_variable = 10


class ModuleLevelClass:

    def class_method(self, x):
        return x * module_level_variable


def module_func(a: float) -> float:
    return a * 5


def module_func_with_deps(a: float, b: float) -> float:
    return ModuleLevelClass().class_method(a) + module_func(b)
