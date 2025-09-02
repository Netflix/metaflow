from typing import Dict, Optional, List, Tuple


class _TrieNode:
    def __init__(
        self, parent: Optional["_TrieNode"] = None, component: Optional[str] = None
    ):
        self.parent = parent
        self.component = component
        self.children = {}  # type: Dict[str, "_TrieNode"]
        self.total_children = 0
        self.value = None
        self.end_value = None

    def traverse(self, value: type) -> Optional["_TrieNode"]:
        if self.total_children == 0:
            self.end_value = value
        else:
            self.end_value = None
        self.total_children += 1

    def remove_child(self, child_name: str) -> bool:
        if child_name in self.children:
            del self.children[child_name]
            self.total_children -= 1
            return True
        return False


class ClassPath_Trie:
    def __init__(self):
        self.root = _TrieNode(None, None)
        self.inited = False
        self._value_to_node = {}  # type: Dict[type, _TrieNode]

    def init(self, initial_nodes: Optional[List[Tuple[str, type]]] = None):
        # We need to do this so we can delay import of STEP_DECORATORS
        self.inited = True
        for classpath_name, value in initial_nodes or []:
            self.insert(classpath_name, value)

    def insert(self, classpath_name: str, value: type):
        node = self.root
        components = reversed(classpath_name.split("."))
        for c in components:
            node = node.children.setdefault(c, _TrieNode(node, c))
            node.traverse(value)
        node.total_children -= (
            1  # We do not count the last node as having itself as a child
        )
        node.value = value
        self._value_to_node[value] = node

    def search(self, classpath_name: str) -> Optional[type]:
        node = self.root
        components = reversed(classpath_name.split("."))
        for c in components:
            if c not in node.children:
                return None
            node = node.children[c]
        return node.value

    def remove(self, classpath_name: str):
        components = list(reversed(classpath_name.split(".")))

        def _remove(node: _TrieNode, components, depth):
            if depth == len(components):
                if node.value is not None:
                    del self._value_to_node[node.value]
                    node.value = None
                    return len(node.children) == 0
                return False
            c = components[depth]
            if c not in node.children:
                return False
            did_delete_child = _remove(node.children[c], components, depth + 1)
            if did_delete_child:
                node.remove_child(c)
                if node.total_children == 1:
                    # If we have one total child left, we have at least one
                    # child and that one has an end_value
                    for child in node.children.values():
                        assert (
                            child.end_value
                        ), "Node with one child must have an end_value"
                        node.end_value = child.end_value
                return node.total_children == 0
            return False

        _remove(self.root, components, 0)

    def unique_prefix_value(self, classpath_name: str) -> Optional[type]:
        node = self.root
        components = reversed(classpath_name.split("."))
        for c in components:
            if c not in node.children:
                return None
            node = node.children[c]
        # If we reach here, it means the classpath_name is a prefix.
        # We check if it has only one path forward (end_value will be non None)
        # If value is not None, we also consider this to be a unique "prefix"
        # This happens since this trie is also filled with metaflow default decorators
        return node.end_value or node.value

    def unique_prefix_for_type(self, value: type) -> Optional[str]:
        node = self._value_to_node.get(value, None)
        if node is None:
            return None
        components = []
        while node:
            if node.end_value == value:
                components = []
            if node.component is not None:
                components.append(node.component)
            node = node.parent
        return ".".join(components)

    def get_unique_prefixes(self) -> Dict[str, type]:
        """
        Get all unique prefixes in the trie.

        Returns
        -------
        List[str]
            A list of unique prefixes.
        """
        to_return = {}

        def _collect(node, current_prefix):
            if node.end_value is not None:
                to_return[current_prefix] = node.end_value
                # We stop there and don't look further since we found the unique prefix
                return
            if node.value is not None:
                to_return[current_prefix] = node.value
                # We continue to look for more unique prefixes
            for child_name, child_node in node.children.items():
                _collect(
                    child_node,
                    f"{current_prefix}.{child_name}" if current_prefix else child_name,
                )

        _collect(self.root, "")
        return {".".join(reversed(k.split("."))): v for k, v in to_return.items()}
