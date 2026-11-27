"""
Example FlowMutator that extends the set of file suffixes included in the code
package — mirrors the ``--package-suffixes`` CLI option as a decorator.

By default Metaflow's packaging walks the flow directory and includes files
whose suffixes are in ``DEFAULT_PACKAGE_SUFFIXES`` (``.py,.R,.RDS``) plus
whatever was passed via ``--package-suffixes``. This mutator lets a flow
author declare additional suffixes directly on the flow class::

    from metaflow import FlowSpec, step
    from metaflow.plugins.package_suffixes_mutator import package_suffixes

    @package_suffixes([".yaml", ".json"])
    class MyFlow(FlowSpec):
        @step
        def start(self):
            ...

The mutator walks the flow directory itself and yields every file with a
matching suffix as ``USER_CONTENT``. The packaging layer deduplicates against
the files that the default walker already picked up, so files that are already
included (e.g. ``.py`` files) are not added twice.
"""

import inspect
import os
from typing import List, Union

from metaflow.packaging_sys import ContentType
from metaflow.packaging_sys.utils import walk
from metaflow.user_decorators.user_flow_decorator import FlowMutator


class package_suffixes(FlowMutator):
    """Include additional file suffixes in the code package.

    Parameters
    ----------
    suffixes : list of str or comma-separated str
        Additional file suffixes to include (e.g. ``[".yaml", ".json"]`` or
        ``".yaml,.json"``). Leading dots are optional; a suffix without a
        leading dot is treated as an extension (``yaml`` → ``.yaml``).
    """

    def init(self, suffixes: Union[List[str], str]):
        if isinstance(suffixes, str):
            suffixes = [s.strip() for s in suffixes.split(",") if s.strip()]
        self._suffixes = tuple(
            (s if s.startswith(".") else "." + s).lower() for s in suffixes
        )

    def add_to_package(self):
        if not self._suffixes:
            return

        try:
            flow_file = inspect.getfile(self._flow_cls)
        except (TypeError, OSError):
            return
        flow_dir = os.path.dirname(os.path.abspath(flow_file))
        if not flow_dir or not os.path.isdir(flow_dir):
            return

        def _filter(fname: str) -> bool:
            lname = fname.lower()
            return any(lname.endswith(sfx) for sfx in self._suffixes)

        for file_path, arcname in walk(flow_dir + os.sep, file_filter=_filter):
            yield (file_path, arcname, ContentType.USER_CONTENT)
