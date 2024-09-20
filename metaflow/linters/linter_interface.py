from abc import ABC
from enum import Enum
from typing import Union

from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.plugins.pypi.conda_environment import CondaEnvironment


class LinterConfig:
    @staticmethod
    def from_enum(environment: Union[MetaflowEnvironment, CondaEnvironment], linter_choice: Enum):
        if linter_choice == LinterType.PYLINT:
            return environment.pylint_config
        elif linter_choice == LinterType.RUFF:
            return environment.ruff_config


class LinterType(Enum):
    PYLINT = 'pylint'
    RUFF = 'ruff'

    @staticmethod
    def from_string(linter_name: str):
        linter_name = linter_name.lower()
        if linter_name == 'pylint':
            return LinterType.PYLINT
        elif linter_name == 'ruff':
            return LinterType.RUFF
        else:
            raise ValueError(f"Unknown linter name: {linter_name}")


class LinterWrapperInterface(ABC):
    def __init__(self):
        pass

    def lint(self, file_path: str, warnings=None, lint_config=None, logger=None) -> None:
        pass


class LinterFactory:
    @staticmethod
    def get_linter(linter_name: LinterType) -> LinterWrapperInterface:
        if linter_name == LinterType.PYLINT:
            from metaflow.linters.pylint.pylint_wrapper import PyLintWrapper
            return PyLintWrapper()
        elif linter_name == LinterType.RUFF:
            from metaflow.linters.ruff.ruff_wrapper import RuffWrapper
            return RuffWrapper()
        else:
            raise ValueError(f"Unknown linter: {linter_name}")
