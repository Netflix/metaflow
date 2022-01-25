# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import collections
from typing import TYPE_CHECKING, Callable, DefaultDict, Dict, List, Optional, Tuple

from metaflow._vendor.pylint.exceptions import EmptyReportError
from metaflow._vendor.pylint.interfaces import IChecker
from metaflow._vendor.pylint.reporters.ureports.nodes import Section
from metaflow._vendor.pylint.utils import LinterStats

if TYPE_CHECKING:
    from metaflow._vendor.pylint.lint.pylinter import PyLinter

ReportsDict = DefaultDict[IChecker, List[Tuple[str, str, Callable]]]


class ReportsHandlerMixIn:
    """a mix-in class containing all the reports and stats manipulation
    related methods for the main lint class
    """

    def __init__(self) -> None:
        self._reports: ReportsDict = collections.defaultdict(list)
        self._reports_state: Dict[str, bool] = {}

    def report_order(self) -> List[IChecker]:
        """Return a list of reporters"""
        return list(self._reports)

    def register_report(
        self, reportid: str, r_title: str, r_cb: Callable, checker: IChecker
    ) -> None:
        """register a report

        reportid is the unique identifier for the report
        r_title the report's title
        r_cb the method to call to make the report
        checker is the checker defining the report
        """
        reportid = reportid.upper()
        self._reports[checker].append((reportid, r_title, r_cb))

    def enable_report(self, reportid: str) -> None:
        """disable the report of the given id"""
        reportid = reportid.upper()
        self._reports_state[reportid] = True

    def disable_report(self, reportid: str) -> None:
        """disable the report of the given id"""
        reportid = reportid.upper()
        self._reports_state[reportid] = False

    def report_is_enabled(self, reportid: str) -> bool:
        """return true if the report associated to the given identifier is
        enabled
        """
        return self._reports_state.get(reportid, True)

    def make_reports(  # type: ignore[misc] # ReportsHandlerMixIn is always mixed with PyLinter
        self: "PyLinter",
        stats: LinterStats,
        old_stats: Optional[LinterStats],
    ) -> Section:
        """render registered reports"""
        sect = Section("Report", f"{self.stats.statement} statements analysed.")
        for checker in self.report_order():
            for reportid, r_title, r_cb in self._reports[checker]:
                if not self.report_is_enabled(reportid):
                    continue
                report_sect = Section(r_title)
                try:
                    r_cb(report_sect, stats, old_stats)
                except EmptyReportError:
                    continue
                report_sect.report_id = reportid
                sect.append(report_sect)
        return sect
