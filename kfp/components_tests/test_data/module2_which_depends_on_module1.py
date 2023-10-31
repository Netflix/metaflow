from .module1 import module_func_with_deps


def module2_func_with_deps(a: float, b: float) -> float:
    return module_func_with_deps(a, b) + 10
