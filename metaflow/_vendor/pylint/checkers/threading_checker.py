# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint import interfaces
from metaflow._vendor.pylint.checkers import BaseChecker
from metaflow._vendor.pylint.checkers.utils import check_messages, safe_infer


class ThreadingChecker(BaseChecker):
    """Checks for threading module

    - useless with lock - locking used in wrong way that has no effect (with threading.Lock():)
    """

    __implements__ = interfaces.IAstroidChecker
    name = "threading"

    LOCKS = frozenset(
        (
            "threading.Lock",
            "threading.RLock",
            "threading.Condition",
            "threading.Semaphore",
            "threading.BoundedSemaphore",
        )
    )

    msgs = {
        "W2101": (
            "'%s()' directly created in 'with' has no effect",
            "useless-with-lock",
            "Used when a new lock instance is created by using with statement "
            "which has no effect. Instead, an existing instance should be used to acquire lock.",
        ),
    }

    @check_messages("useless-with-lock")
    def visit_with(self, node: nodes.With) -> None:

        context_managers = (c for c, _ in node.items if isinstance(c, nodes.Call))
        for context_manager in context_managers:
            if isinstance(context_manager, nodes.Call):
                infered_function = safe_infer(context_manager.func)
                if infered_function is None:
                    continue
                qname = infered_function.qname()
                if qname in self.LOCKS:
                    self.add_message("useless-with-lock", node=node, args=qname)


def register(linter):
    """required method to auto register this checker"""
    linter.register_checker(ThreadingChecker(linter))
