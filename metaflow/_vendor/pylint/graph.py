# Copyright (c) 2015-2018, 2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015 Florian Bruhin <me@the-compiler.org>
# Copyright (c) 2016 Ashley Whetter <ashley@awhetter.co.uk>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2019, 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2020 Damien Baty <damien.baty@polyconseil.fr>
# Copyright (c) 2020 谭九鼎 <109224573@qq.com>
# Copyright (c) 2020 Benjamin Graham <benwilliamgraham@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>
# Copyright (c) 2021 Andreas Finkler <andi.finkler@gmail.com>
# Copyright (c) 2021 Andrew Howe <howeaj@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Graph manipulation utilities.

(dot generation adapted from pypy/translator/tool/make_dot.py)
"""
import codecs
import os
import shutil
import subprocess
import sys
import tempfile
from typing import Optional


def target_info_from_filename(filename):
    """Transforms /some/path/foo.png into ('/some/path', 'foo.png', 'png')."""
    basename = os.path.basename(filename)
    storedir = os.path.dirname(os.path.abspath(filename))
    target = os.path.splitext(filename)[-1][1:]
    return storedir, basename, target


class DotBackend:
    """Dot File backend."""

    def __init__(
        self,
        graphname,
        rankdir=None,
        size=None,
        ratio=None,
        charset="utf-8",
        renderer="dot",
        additional_param=None,
    ):
        if additional_param is None:
            additional_param = {}
        self.graphname = graphname
        self.renderer = renderer
        self.lines = []
        self._source = None
        self.emit(f"digraph {normalize_node_id(graphname)} {{")
        if rankdir:
            self.emit(f"rankdir={rankdir}")
        if ratio:
            self.emit(f"ratio={ratio}")
        if size:
            self.emit(f'size="{size}"')
        if charset:
            assert charset.lower() in {
                "utf-8",
                "iso-8859-1",
                "latin1",
            }, f"unsupported charset {charset}"
            self.emit(f'charset="{charset}"')
        for param in additional_param.items():
            self.emit("=".join(param))

    def get_source(self):
        """returns self._source"""
        if self._source is None:
            self.emit("}\n")
            self._source = "\n".join(self.lines)
            del self.lines
        return self._source

    source = property(get_source)

    def generate(
        self, outputfile: Optional[str] = None, mapfile: Optional[str] = None
    ) -> str:
        """Generates a graph file.

        :param str outputfile: filename and path [defaults to graphname.png]
        :param str mapfile: filename and path

        :rtype: str
        :return: a path to the generated file
        :raises RuntimeError: if the executable for rendering was not found
        """
        graphviz_extensions = ("dot", "gv")
        name = self.graphname
        if outputfile is None:
            target = "png"
            pdot, dot_sourcepath = tempfile.mkstemp(".gv", name)
            ppng, outputfile = tempfile.mkstemp(".png", name)
            os.close(pdot)
            os.close(ppng)
        else:
            _, _, target = target_info_from_filename(outputfile)
            if not target:
                target = "png"
                outputfile = outputfile + "." + target
            if target not in graphviz_extensions:
                pdot, dot_sourcepath = tempfile.mkstemp(".gv", name)
                os.close(pdot)
            else:
                dot_sourcepath = outputfile
        with codecs.open(dot_sourcepath, "w", encoding="utf8") as file:
            file.write(self.source)
        if target not in graphviz_extensions:
            if shutil.which(self.renderer) is None:
                raise RuntimeError(
                    f"Cannot generate `{outputfile}` because '{self.renderer}' "
                    "executable not found. Install graphviz, or specify a `.gv` "
                    "outputfile to produce the DOT source code."
                )
            use_shell = sys.platform == "win32"
            if mapfile:
                subprocess.call(
                    [
                        self.renderer,
                        "-Tcmapx",
                        "-o",
                        mapfile,
                        "-T",
                        target,
                        dot_sourcepath,
                        "-o",
                        outputfile,
                    ],
                    shell=use_shell,
                )
            else:
                subprocess.call(
                    [self.renderer, "-T", target, dot_sourcepath, "-o", outputfile],
                    shell=use_shell,
                )
            os.unlink(dot_sourcepath)
        return outputfile

    def emit(self, line):
        """Adds <line> to final output."""
        self.lines.append(line)

    def emit_edge(self, name1, name2, **props):
        """emit an edge from <name1> to <name2>.
        edge properties: see https://www.graphviz.org/doc/info/attrs.html
        """
        attrs = [f'{prop}="{value}"' for prop, value in props.items()]
        n_from, n_to = normalize_node_id(name1), normalize_node_id(name2)
        self.emit(f"{n_from} -> {n_to} [{', '.join(sorted(attrs))}];")

    def emit_node(self, name, **props):
        """emit a node with given properties.
        node properties: see https://www.graphviz.org/doc/info/attrs.html
        """
        attrs = [f'{prop}="{value}"' for prop, value in props.items()]
        self.emit(f"{normalize_node_id(name)} [{', '.join(sorted(attrs))}];")


def normalize_node_id(nid):
    """Returns a suitable DOT node id for `nid`."""
    return f'"{nid}"'


def get_cycles(graph_dict, vertices=None):
    """given a dictionary representing an ordered graph (i.e. key are vertices
    and values is a list of destination vertices representing edges), return a
    list of detected cycles
    """
    if not graph_dict:
        return ()
    result = []
    if vertices is None:
        vertices = graph_dict.keys()
    for vertice in vertices:
        _get_cycles(graph_dict, [], set(), result, vertice)
    return result


def _get_cycles(graph_dict, path, visited, result, vertice):
    """recursive function doing the real work for get_cycles"""
    if vertice in path:
        cycle = [vertice]
        for node in path[::-1]:
            if node == vertice:
                break
            cycle.insert(0, node)
        # make a canonical representation
        start_from = min(cycle)
        index = cycle.index(start_from)
        cycle = cycle[index:] + cycle[0:index]
        # append it to result if not already in
        if cycle not in result:
            result.append(cycle)
        return
    path.append(vertice)
    try:
        for node in graph_dict[vertice]:
            # don't check already visited nodes again
            if node not in visited:
                _get_cycles(graph_dict, path, visited, result, node)
                visited.add(node)
    except KeyError:
        pass
    path.pop()
