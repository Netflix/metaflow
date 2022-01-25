# Copyright (c) 2015-2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2019, 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Universal report objects and some formatting drivers.

A way to create simple reports using python objects, primarily designed to be
formatted as text and html.
"""
import sys
from io import StringIO
from typing import TYPE_CHECKING, Iterator, List, TextIO, Union

if TYPE_CHECKING:
    from metaflow._vendor.pylint.reporters.ureports.nodes import (
        EvaluationSection,
        Paragraph,
        Section,
        Table,
    )


class BaseWriter:
    """base class for ureport writers"""

    def format(self, layout, stream: TextIO = sys.stdout, encoding=None) -> None:
        """format and write the given layout into the stream object

        unicode policy: unicode strings may be found in the layout;
        try to call stream.write with it, but give it back encoded using
        the given encoding if it fails
        """
        if not encoding:
            encoding = getattr(stream, "encoding", "UTF-8")
        self.encoding = encoding or "UTF-8"
        self.out = stream
        self.begin_format()
        layout.accept(self)
        self.end_format()

    def format_children(
        self, layout: Union["EvaluationSection", "Paragraph", "Section"]
    ) -> None:
        """recurse on the layout children and call their accept method
        (see the Visitor pattern)
        """
        for child in getattr(layout, "children", ()):
            child.accept(self)

    def writeln(self, string: str = "") -> None:
        """write a line in the output buffer"""
        self.write(string + "\n")

    def write(self, string: str) -> None:
        """write a string in the output buffer"""
        self.out.write(string)

    def begin_format(self) -> None:
        """begin to format a layout"""
        self.section = 0

    def end_format(self) -> None:
        """finished to format a layout"""

    def get_table_content(self, table: "Table") -> List[List[str]]:
        """trick to get table content without actually writing it

        return an aligned list of lists containing table cells values as string
        """
        result: List[List[str]] = [[]]
        cols = table.cols
        for cell in self.compute_content(table):
            if cols == 0:
                result.append([])
                cols = table.cols
            cols -= 1
            result[-1].append(cell)
        # fill missing cells
        result[-1] += [""] * (cols - len(result[-1]))
        return result

    def compute_content(self, layout) -> Iterator[str]:
        """trick to compute the formatting of children layout before actually
        writing it

        return an iterator on strings (one for each child element)
        """
        # Patch the underlying output stream with a fresh-generated stream,
        # which is used to store a temporary representation of a child
        # node.
        out = self.out
        try:
            for child in layout.children:
                stream = StringIO()
                self.out = stream
                child.accept(self)
                yield stream.getvalue()
        finally:
            self.out = out
