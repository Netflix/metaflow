# Copyright (c) 2008-2010, 2012-2014 LOGILAB S.A. (Paris, FRANCE) <contact@logilab.fr>
# Copyright (c) 2014 Brett Cannon <brett@python.org>
# Copyright (c) 2014 Arun Persaud <arun@nubati.net>
# Copyright (c) 2015-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2015 Ionel Cristian Maries <contact@ionelmc.ro>
# Copyright (c) 2016 Alexander Pervakov <frost.nzcr4@jagmort.com>
# Copyright (c) 2018 ssolanki <sushobhitsolanki@gmail.com>
# Copyright (c) 2019, 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019 Hugo van Kemenade <hugovk@users.noreply.github.com>
# Copyright (c) 2020 Peter Kolbus <peter.kolbus@gmail.com>
# Copyright (c) 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2021 Antonio Quarta <sgheppy88@gmail.com>
# Copyright (c) 2021 Tushar Sadhwani <tushar.sadhwani000@gmail.com>
# Copyright (c) 2021 Mark Byrne <31762852+mbyrnepr2@users.noreply.github.com>
# Copyright (c) 2021 bot <bot@noreply.github.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Andreas Finkler <andi.finkler@gmail.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""
  %prog [options] <packages>

  create UML diagrams for classes and modules in <packages>
"""
import sys
from typing import Iterable

from metaflow._vendor.pylint.config import ConfigurationMixIn
from metaflow._vendor.pylint.lint.utils import fix_import_path
from metaflow._vendor.pylint.pyreverse import writer
from metaflow._vendor.pylint.pyreverse.diadefslib import DiadefsHandler
from metaflow._vendor.pylint.pyreverse.inspector import Linker, project_from_files
from metaflow._vendor.pylint.pyreverse.utils import check_graphviz_availability, insert_default_options

OPTIONS = (
    (
        "filter-mode",
        dict(
            short="f",
            default="PUB_ONLY",
            dest="mode",
            type="string",
            action="store",
            metavar="<mode>",
            help="""filter attributes and functions according to
    <mode>. Correct modes are :
                            'PUB_ONLY' filter all non public attributes
                                [DEFAULT], equivalent to PRIVATE+SPECIAL_A
                            'ALL' no filter
                            'SPECIAL' filter Python special functions
                                except constructor
                            'OTHER' filter protected and private
                                attributes""",
        ),
    ),
    (
        "class",
        dict(
            short="c",
            action="append",
            metavar="<class>",
            dest="classes",
            default=[],
            help="create a class diagram with all classes related to <class>;\
 this uses by default the options -ASmy",
        ),
    ),
    (
        "show-ancestors",
        dict(
            short="a",
            action="store",
            metavar="<ancestor>",
            type="int",
            help="show <ancestor> generations of ancestor classes not in <projects>",
        ),
    ),
    (
        "all-ancestors",
        dict(
            short="A",
            default=None,
            help="show all ancestors off all classes in <projects>",
        ),
    ),
    (
        "show-associated",
        dict(
            short="s",
            action="store",
            metavar="<association_level>",
            type="int",
            help="show <association_level> levels of associated classes not in <projects>",
        ),
    ),
    (
        "all-associated",
        dict(
            short="S",
            default=None,
            help="show recursively all associated off all associated classes",
        ),
    ),
    (
        "show-builtin",
        dict(
            short="b",
            action="store_true",
            default=False,
            help="include builtin objects in representation of classes",
        ),
    ),
    (
        "module-names",
        dict(
            short="m",
            default=None,
            type="yn",
            metavar="<y or n>",
            help="include module name in representation of classes",
        ),
    ),
    (
        "only-classnames",
        dict(
            short="k",
            action="store_true",
            default=False,
            help="don't show attributes and methods in the class boxes; this disables -f values",
        ),
    ),
    (
        "output",
        dict(
            short="o",
            dest="output_format",
            action="store",
            default="dot",
            metavar="<format>",
            help="create a *.<format> output file if format available.",
        ),
    ),
    (
        "colorized",
        dict(
            dest="colorized",
            action="store_true",
            default=False,
            help="Use colored output. Classes/modules of the same package get the same color.",
        ),
    ),
    (
        "max-color-depth",
        dict(
            dest="max_color_depth",
            action="store",
            default=2,
            metavar="<depth>",
            type="int",
            help="Use separate colors up to package depth of <depth>",
        ),
    ),
    (
        "ignore",
        dict(
            type="csv",
            metavar="<file[,file...]>",
            dest="ignore_list",
            default=("CVS",),
            help="Files or directories to be skipped. They should be base names, not paths.",
        ),
    ),
    (
        "project",
        dict(
            default="",
            type="string",
            short="p",
            metavar="<project name>",
            help="set the project name.",
        ),
    ),
    (
        "output-directory",
        dict(
            default="",
            type="string",
            short="d",
            action="store",
            metavar="<output_directory>",
            help="set the output directory path.",
        ),
    ),
)


class Run(ConfigurationMixIn):
    """base class providing common behaviour for pyreverse commands"""

    options = OPTIONS

    def __init__(self, args: Iterable[str]):
        super().__init__(usage=__doc__)
        insert_default_options()
        args = self.load_command_line_configuration(args)
        if self.config.output_format not in ("dot", "vcg", "puml", "plantuml"):
            check_graphviz_availability()

        sys.exit(self.run(args))

    def run(self, args):
        """checking arguments and run project"""
        if not args:
            print(self.help())
            return 1
        with fix_import_path(args):
            project = project_from_files(
                args,
                project_name=self.config.project,
                black_list=self.config.ignore_list,
            )
        linker = Linker(project, tag=True)
        handler = DiadefsHandler(self.config)
        diadefs = handler.get_diadefs(project, linker)
        writer.DiagramWriter(self.config).write(diadefs)
        return 0


if __name__ == "__main__":
    Run(sys.argv[1:])
