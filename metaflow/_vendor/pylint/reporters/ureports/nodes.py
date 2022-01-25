# Copyright (c) 2015-2016, 2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2018 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Micro reports objects.

A micro report is a tree of layout and content objects.
"""
from typing import Any, Iterable, Iterator, List, Optional, Union

from metaflow._vendor.pylint.reporters.ureports.text_writer import TextWriter


class VNode:
    def __init__(self) -> None:
        self.parent: Optional["BaseLayout"] = None
        self.children: List["VNode"] = []
        self.visitor_name: str = self.__class__.__name__.lower()

    def __iter__(self) -> Iterator["VNode"]:
        return iter(self.children)

    def accept(self, visitor: TextWriter, *args: Any, **kwargs: Any) -> None:
        func = getattr(visitor, f"visit_{self.visitor_name}")
        return func(self, *args, **kwargs)

    def leave(self, visitor, *args, **kwargs):
        func = getattr(visitor, f"leave_{self.visitor_name}")
        return func(self, *args, **kwargs)


class BaseLayout(VNode):
    """base container node

    attributes
    * children : components in this table (i.e. the table's cells)
    """

    def __init__(self, children: Iterable[Union["Text", str]] = ()) -> None:
        super().__init__()
        for child in children:
            if isinstance(child, VNode):
                self.append(child)
            else:
                self.add_text(child)

    def append(self, child: VNode) -> None:
        """add a node to children"""
        assert child not in self.parents()
        self.children.append(child)
        child.parent = self

    def insert(self, index: int, child: VNode) -> None:
        """insert a child node"""
        self.children.insert(index, child)
        child.parent = self

    def parents(self) -> List["BaseLayout"]:
        """return the ancestor nodes"""
        assert self.parent is not self
        if self.parent is None:
            return []
        return [self.parent] + self.parent.parents()

    def add_text(self, text: str) -> None:
        """shortcut to add text data"""
        self.children.append(Text(text))


# non container nodes #########################################################


class Text(VNode):
    """a text portion

    attributes :
    * data : the text value as an encoded or unicode string
    """

    def __init__(self, data: str, escaped: bool = True) -> None:
        super().__init__()
        self.escaped = escaped
        self.data = data


class VerbatimText(Text):
    """a verbatim text, display the raw data

    attributes :
    * data : the text value as an encoded or unicode string
    """


# container nodes #############################################################


class Section(BaseLayout):
    """a section

    attributes :
    * BaseLayout attributes

    a title may also be given to the constructor, it'll be added
    as a first element
    a description may also be given to the constructor, it'll be added
    as a first paragraph
    """

    def __init__(
        self,
        title: Optional[str] = None,
        description: Optional[str] = None,
        children: Iterable[Union["Text", str]] = (),
    ) -> None:
        super().__init__(children=children)
        if description:
            self.insert(0, Paragraph([Text(description)]))
        if title:
            self.insert(0, Title(children=(title,)))
        self.report_id: str = ""  # Used in ReportHandlerMixin.make_reports


class EvaluationSection(Section):
    def __init__(
        self, message: str, children: Iterable[Union["Text", str]] = ()
    ) -> None:
        super().__init__(children=children)
        title = Paragraph()
        title.append(Text("-" * len(message)))
        self.append(title)
        message_body = Paragraph()
        message_body.append(Text(message))
        self.append(message_body)


class Title(BaseLayout):
    """a title

    attributes :
    * BaseLayout attributes

    A title must not contains a section nor a paragraph!
    """


class Paragraph(BaseLayout):
    """a simple text paragraph

    attributes :
    * BaseLayout attributes

    A paragraph must not contains a section !
    """


class Table(BaseLayout):
    """some tabular data

    attributes :
    * BaseLayout attributes
    * cols : the number of columns of the table (REQUIRED)
    * rheaders : the first row's elements are table's header
    * cheaders : the first col's elements are table's header
    * title : the table's optional title
    """

    def __init__(
        self,
        cols: int,
        title: Optional[str] = None,
        rheaders: int = 0,
        cheaders: int = 0,
        children: Iterable[Union["Text", str]] = (),
    ) -> None:
        super().__init__(children=children)
        assert isinstance(cols, int)
        self.cols = cols
        self.title = title
        self.rheaders = rheaders
        self.cheaders = cheaders
