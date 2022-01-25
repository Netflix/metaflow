# Copyright (c) 2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2021 Daniël van Noord <13665637+DanielNoord@users.noreply.github.com>
# Copyright (c) 2021 Andreas Finkler <andi.finkler@gmail.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

from typing import Dict, Type

from metaflow._vendor.pylint.pyreverse.dot_printer import DotPrinter
from metaflow._vendor.pylint.pyreverse.plantuml_printer import PlantUmlPrinter
from metaflow._vendor.pylint.pyreverse.printer import Printer
from metaflow._vendor.pylint.pyreverse.vcg_printer import VCGPrinter

filetype_to_printer: Dict[str, Type[Printer]] = {
    "vcg": VCGPrinter,
    "plantuml": PlantUmlPrinter,
    "puml": PlantUmlPrinter,
    "dot": DotPrinter,
}


def get_printer_for_filetype(filetype: str) -> Type[Printer]:
    return filetype_to_printer.get(filetype, DotPrinter)
