from .pylint import PyLint
from .ruff import Ruff

PYTHON_LINTERS = [
    "pylint",
    "ruff",
]


def get_python_linter(linter):
    if linter == "pylint":
        return PyLint
    elif linter == "ruff":
        return Ruff
    else:
        raise Exception("Unknown linter %s" % linter)
