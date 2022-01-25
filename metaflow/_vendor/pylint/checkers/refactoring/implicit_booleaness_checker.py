# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE
from typing import List

from metaflow._vendor import astroid
from metaflow._vendor.astroid import nodes

from metaflow._vendor.pylint import checkers, interfaces
from metaflow._vendor.pylint.checkers import utils


class ImplicitBooleanessChecker(checkers.BaseChecker):
    """Checks for incorrect usage of comparisons or len() inside conditions.

    Incorrect usage of len()
    Pep8 states:
    For sequences, (strings, lists, tuples), use the fact that empty sequences are false.

        Yes: if not seq:
             if seq:

        No: if len(seq):
            if not len(seq):

    Problems detected:
    * if len(sequence):
    * if not len(sequence):
    * elif len(sequence):
    * elif not len(sequence):
    * while len(sequence):
    * while not len(sequence):
    * assert len(sequence):
    * assert not len(sequence):
    * bool(len(sequence))

    Incorrect usage of empty literal sequences; (), [], {},

    For empty sequences, (dicts, lists, tuples), use the fact that empty sequences are false.

        Yes: if variable:
             if not variable

        No: if variable == empty_literal:
            if variable != empty_literal:

    Problems detected:
    * comparison such as variable == empty_literal:
    * comparison such as variable != empty_literal:
    """

    __implements__ = (interfaces.IAstroidChecker,)

    # configuration section name
    name = "refactoring"
    msgs = {
        "C1802": (
            "Do not use `len(SEQUENCE)` without comparison to determine if a sequence is empty",
            "use-implicit-booleaness-not-len",
            "Used when Pylint detects that len(sequence) is being used "
            "without explicit comparison inside a condition to determine if a sequence is empty. "
            "Instead of coercing the length to a boolean, either "
            "rely on the fact that empty sequences are false or "
            "compare the length against a scalar.",
            {"old_names": [("C1801", "len-as-condition")]},
        ),
        "C1803": (
            "'%s' can be simplified to '%s' as an empty sequence is falsey",
            "use-implicit-booleaness-not-comparison",
            "Used when Pylint detects that collection literal comparison is being "
            "used to check for emptiness; Use implicit booleaness instead"
            "of a collection classes; empty collections are considered as false",
        ),
    }

    priority = -2
    options = ()

    @utils.check_messages("use-implicit-booleaness-not-len")
    def visit_call(self, node: nodes.Call) -> None:
        # a len(S) call is used inside a test condition
        # could be if, while, assert or if expression statement
        # e.g. `if len(S):`
        if not utils.is_call_of_name(node, "len"):
            return
        # the len() call could also be nested together with other
        # boolean operations, e.g. `if z or len(x):`
        parent = node.parent
        while isinstance(parent, nodes.BoolOp):
            parent = parent.parent
        # we're finally out of any nested boolean operations so check if
        # this len() call is part of a test condition
        if not utils.is_test_condition(node, parent):
            return
        len_arg = node.args[0]
        generator_or_comprehension = (
            nodes.ListComp,
            nodes.SetComp,
            nodes.DictComp,
            nodes.GeneratorExp,
        )
        if isinstance(len_arg, generator_or_comprehension):
            # The node is a generator or comprehension as in len([x for x in ...])
            self.add_message("use-implicit-booleaness-not-len", node=node)
            return
        try:
            instance = next(len_arg.infer())
        except astroid.InferenceError:
            # Probably undefined-variable, abort check
            return
        mother_classes = self.base_classes_of_node(instance)
        affected_by_pep8 = any(
            t in mother_classes for t in ("str", "tuple", "list", "set")
        )
        if "range" in mother_classes or (
            affected_by_pep8 and not self.instance_has_bool(instance)
        ):
            self.add_message("use-implicit-booleaness-not-len", node=node)

    @staticmethod
    def instance_has_bool(class_def: nodes.ClassDef) -> bool:
        try:
            class_def.getattr("__bool__")
            return True
        except astroid.AttributeInferenceError:
            ...
        return False

    @utils.check_messages("use-implicit-booleaness-not-len")
    def visit_unaryop(self, node: nodes.UnaryOp) -> None:
        """`not len(S)` must become `not S` regardless if the parent block
        is a test condition or something else (boolean expression)
        e.g. `if not len(S):`"""
        if (
            isinstance(node, nodes.UnaryOp)
            and node.op == "not"
            and utils.is_call_of_name(node.operand, "len")
        ):
            self.add_message("use-implicit-booleaness-not-len", node=node)

    @utils.check_messages("use-implicit-booleaness-not-comparison")
    def visit_compare(self, node: nodes.Compare) -> None:
        self._check_use_implicit_booleaness_not_comparison(node)

    def _check_use_implicit_booleaness_not_comparison(
        self, node: nodes.Compare
    ) -> None:
        """Check for left side and right side of the node for empty literals"""
        is_left_empty_literal = utils.is_base_container(
            node.left
        ) or utils.is_empty_dict_literal(node.left)

        # Check both left hand side and right hand side for literals
        for operator, comparator in node.ops:
            is_right_empty_literal = utils.is_base_container(
                comparator
            ) or utils.is_empty_dict_literal(comparator)
            # Using Exclusive OR (XOR) to compare between two side.
            # If two sides are both literal, it should be different error.
            if is_right_empty_literal ^ is_left_empty_literal:
                # set target_node to opposite side of literal
                target_node = node.left if is_right_empty_literal else comparator
                literal_node = comparator if is_right_empty_literal else node.left
                # Infer node to check
                target_instance = utils.safe_infer(target_node)
                if target_instance is None:
                    continue
                mother_classes = self.base_classes_of_node(target_instance)
                is_base_comprehension_type = any(
                    t in mother_classes for t in ("tuple", "list", "dict", "set")
                )

                # Only time we bypass check is when target_node is not inherited by
                # collection literals and have its own __bool__ implementation.
                if not is_base_comprehension_type and self.instance_has_bool(
                    target_instance
                ):
                    continue

                # No need to check for operator when visiting compare node
                if operator in {"==", "!=", ">=", ">", "<=", "<"}:
                    collection_literal = "{}"
                    if isinstance(literal_node, nodes.List):
                        collection_literal = "[]"
                    if isinstance(literal_node, nodes.Tuple):
                        collection_literal = "()"

                    instance_name = "x"
                    if isinstance(target_node, nodes.Call) and target_node.func:
                        instance_name = f"{target_node.func.as_string()}(...)"
                    elif isinstance(target_node, (nodes.Attribute, nodes.Name)):
                        instance_name = target_node.as_string()

                    original_comparison = (
                        f"{instance_name} {operator} {collection_literal}"
                    )
                    suggestion = (
                        f"{instance_name}"
                        if operator == "!="
                        else f"not {instance_name}"
                    )
                    self.add_message(
                        "use-implicit-booleaness-not-comparison",
                        args=(
                            original_comparison,
                            suggestion,
                        ),
                        node=node,
                    )

    @staticmethod
    def base_classes_of_node(instance: nodes.ClassDef) -> List[str]:
        """Return all the classes names that a ClassDef inherit from including 'object'."""
        try:
            return [instance.name] + [x.name for x in instance.ancestors()]
        except TypeError:
            return [instance.name]
