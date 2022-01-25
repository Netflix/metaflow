# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

from typing import List, Optional, Tuple


# This class could and should be replaced with a simple dataclass when support for Python < 3.7 is dropped.
# A NamedTuple is not possible as some tests need to modify attributes during the test.
class PyreverseConfig:  # pylint: disable=too-many-instance-attributes, too-many-arguments
    """Holds the configuration options for Pyreverse.
    The default values correspond to the defaults of the options parser."""

    def __init__(
        self,
        mode: str = "PUB_ONLY",
        classes: Optional[List[str]] = None,
        show_ancestors: Optional[int] = None,
        all_ancestors: Optional[bool] = None,
        show_associated: Optional[int] = None,
        all_associated: Optional[bool] = None,
        show_builtin: bool = False,
        module_names: Optional[bool] = None,
        only_classnames: bool = False,
        output_format: str = "dot",
        colorized: bool = False,
        max_color_depth: int = 2,
        ignore_list: Tuple[str, ...] = tuple(),
        project: str = "",
        output_directory: str = "",
    ) -> None:
        self.mode = mode
        if classes:
            self.classes = classes
        else:
            self.classes = []
        self.show_ancestors = show_ancestors
        self.all_ancestors = all_ancestors
        self.show_associated = show_associated
        self.all_associated = all_associated
        self.show_builtin = show_builtin
        self.module_names = module_names
        self.only_classnames = only_classnames
        self.output_format = output_format
        self.colorized = colorized
        self.max_color_depth = max_color_depth
        self.ignore_list = ignore_list
        self.project = project
        self.output_directory = output_directory
