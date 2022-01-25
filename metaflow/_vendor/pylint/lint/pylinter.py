# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import collections
import contextlib
import functools
import operator
import os
import sys
import tokenize
import traceback
import warnings
from io import TextIOWrapper
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Union

from metaflow._vendor import astroid
from metaflow._vendor.astroid import AstroidError, nodes

from metaflow._vendor.pylint import checkers, config, exceptions, interfaces, reporters
from metaflow._vendor.pylint.constants import (
    MAIN_CHECKER_NAME,
    MSG_STATE_CONFIDENCE,
    MSG_STATE_SCOPE_CONFIG,
    MSG_STATE_SCOPE_MODULE,
    MSG_TYPES,
    MSG_TYPES_LONG,
    MSG_TYPES_STATUS,
)
from metaflow._vendor.pylint.lint.expand_modules import expand_modules
from metaflow._vendor.pylint.lint.parallel import check_parallel
from metaflow._vendor.pylint.lint.report_functions import (
    report_messages_by_module_stats,
    report_messages_stats,
    report_total_messages_stats,
)
from metaflow._vendor.pylint.lint.utils import (
    fix_import_path,
    get_fatal_error_message,
    prepare_crash_report,
)
from metaflow._vendor.pylint.message import Message, MessageDefinition, MessageDefinitionStore
from metaflow._vendor.pylint.reporters.ureports import nodes as report_nodes
from metaflow._vendor.pylint.typing import (
    FileItem,
    ManagedMessage,
    MessageLocationTuple,
    ModuleDescriptionDict,
)
from metaflow._vendor.pylint.utils import ASTWalker, FileState, LinterStats, get_global_option, utils
from metaflow._vendor.pylint.utils.pragma_parser import (
    OPTION_PO,
    InvalidPragmaError,
    UnRecognizedOptionError,
    parse_pragma,
)

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from metaflow._vendor.typing_extensions import Literal

MANAGER = astroid.MANAGER


def _read_stdin():
    # https://mail.python.org/pipermail/python-list/2012-November/634424.html
    sys.stdin = TextIOWrapper(sys.stdin.detach(), encoding="utf-8")
    return sys.stdin.read()


def _load_reporter_by_class(reporter_class: str) -> type:
    qname = reporter_class
    module_part = astroid.modutils.get_module_part(qname)
    module = astroid.modutils.load_module_from_name(module_part)
    class_name = qname.split(".")[-1]
    return getattr(module, class_name)


# Python Linter class #########################################################

MSGS = {
    "F0001": (
        "%s",
        "fatal",
        "Used when an error occurred preventing the analysis of a \
              module (unable to find it for instance).",
    ),
    "F0002": (
        "%s: %s",
        "astroid-error",
        "Used when an unexpected error occurred while building the "
        "Astroid  representation. This is usually accompanied by a "
        "traceback. Please report such errors !",
    ),
    "F0010": (
        "error while code parsing: %s",
        "parse-error",
        "Used when an exception occurred while building the Astroid "
        "representation which could be handled by astroid.",
    ),
    "F0011": (
        "error while parsing the configuration: %s",
        "config-parse-error",
        "Used when an exception occurred while parsing a pylint configuration file.",
    ),
    "I0001": (
        "Unable to run raw checkers on built-in module %s",
        "raw-checker-failed",
        "Used to inform that a built-in module has not been checked "
        "using the raw checkers.",
    ),
    "I0010": (
        "Unable to consider inline option %r",
        "bad-inline-option",
        "Used when an inline option is either badly formatted or can't "
        "be used inside modules.",
    ),
    "I0011": (
        "Locally disabling %s (%s)",
        "locally-disabled",
        "Used when an inline option disables a message or a messages category.",
    ),
    "I0013": (
        "Ignoring entire file",
        "file-ignored",
        "Used to inform that the file will not be checked",
    ),
    "I0020": (
        "Suppressed %s (from line %d)",
        "suppressed-message",
        "A message was triggered on a line, but suppressed explicitly "
        "by a disable= comment in the file. This message is not "
        "generated for messages that are ignored due to configuration "
        "settings.",
    ),
    "I0021": (
        "Useless suppression of %s",
        "useless-suppression",
        "Reported when a message is explicitly disabled for a line or "
        "a block of code, but never triggered.",
    ),
    "I0022": (
        'Pragma "%s" is deprecated, use "%s" instead',
        "deprecated-pragma",
        "Some inline pylint options have been renamed or reworked, "
        "only the most recent form should be used. "
        "NOTE:skip-all is only available with pylint >= 0.26",
        {"old_names": [("I0014", "deprecated-disable-all")]},
    ),
    "E0001": ("%s", "syntax-error", "Used when a syntax error is raised for a module."),
    "E0011": (
        "Unrecognized file option %r",
        "unrecognized-inline-option",
        "Used when an unknown inline option is encountered.",
    ),
    "E0012": (
        "Bad option value %r",
        "bad-option-value",
        "Used when a bad value for an inline option is encountered.",
    ),
    "E0013": (
        "Plugin '%s' is impossible to load, is it installed ? ('%s')",
        "bad-plugin-value",
        "Used when a bad value is used in 'load-plugins'.",
    ),
    "E0014": (
        "Out-of-place setting encountered in top level configuration-section '%s' : '%s'",
        "bad-configuration-section",
        "Used when we detect a setting in the top level of a toml configuration that shouldn't be there.",
    ),
}


# pylint: disable=too-many-instance-attributes,too-many-public-methods
class PyLinter(
    config.OptionsManagerMixIn,
    reporters.ReportsHandlerMixIn,
    checkers.BaseTokenChecker,
):
    """lint Python modules using external checkers.

    This is the main checker controlling the other ones and the reports
    generation. It is itself both a raw checker and an astroid checker in order
    to:
    * handle message activation / deactivation at the module level
    * handle some basic but necessary stats'data (number of classes, methods...)

    IDE plugin developers: you may have to call
    `astroid.builder.MANAGER.astroid_cache.clear()` across runs if you want
    to ensure the latest code version is actually checked.

    This class needs to support pickling for parallel linting to work. The exception
    is reporter member; see check_parallel function for more details.
    """

    __implements__ = (interfaces.ITokenChecker,)

    name = MAIN_CHECKER_NAME
    priority = 0
    level = 0
    msgs = MSGS
    # Will be used like this : datetime.now().strftime(crash_file_path)
    crash_file_path: str = "pylint-crash-%Y-%m-%d-%H.txt"

    @staticmethod
    def make_options():
        return (
            (
                "ignore",
                {
                    "type": "csv",
                    "metavar": "<file>[,<file>...]",
                    "dest": "black_list",
                    "default": ("CVS",),
                    "help": "Files or directories to be skipped. "
                    "They should be base names, not paths.",
                },
            ),
            (
                "ignore-patterns",
                {
                    "type": "regexp_csv",
                    "metavar": "<pattern>[,<pattern>...]",
                    "dest": "black_list_re",
                    "default": (),
                    "help": "Files or directories matching the regex patterns are"
                    " skipped. The regex matches against base names, not paths.",
                },
            ),
            (
                "ignore-paths",
                {
                    "type": "regexp_paths_csv",
                    "metavar": "<pattern>[,<pattern>...]",
                    "default": [],
                    "help": "Add files or directories matching the regex patterns to the "
                    "ignore-list. The regex matches against paths and can be in "
                    "Posix or Windows format.",
                },
            ),
            (
                "persistent",
                {
                    "default": True,
                    "type": "yn",
                    "metavar": "<y or n>",
                    "level": 1,
                    "help": "Pickle collected data for later comparisons.",
                },
            ),
            (
                "load-plugins",
                {
                    "type": "csv",
                    "metavar": "<modules>",
                    "default": (),
                    "level": 1,
                    "help": "List of plugins (as comma separated values of "
                    "python module names) to load, usually to register "
                    "additional checkers.",
                },
            ),
            (
                "output-format",
                {
                    "default": "text",
                    "type": "string",
                    "metavar": "<format>",
                    "short": "f",
                    "group": "Reports",
                    "help": "Set the output format. Available formats are text,"
                    " parseable, colorized, json and msvs (visual studio)."
                    " You can also give a reporter class, e.g. mypackage.mymodule."
                    "MyReporterClass.",
                },
            ),
            (
                "reports",
                {
                    "default": False,
                    "type": "yn",
                    "metavar": "<y or n>",
                    "short": "r",
                    "group": "Reports",
                    "help": "Tells whether to display a full report or only the "
                    "messages.",
                },
            ),
            (
                "evaluation",
                {
                    "type": "string",
                    "metavar": "<python_expression>",
                    "group": "Reports",
                    "level": 1,
                    "default": "10.0 - ((float(5 * error + warning + refactor + "
                    "convention) / statement) * 10)",
                    "help": "Python expression which should return a score less "
                    "than or equal to 10. You have access to the variables "
                    "'error', 'warning', 'refactor', and 'convention' which "
                    "contain the number of messages in each category, as well as "
                    "'statement' which is the total number of statements "
                    "analyzed. This score is used by the global "
                    "evaluation report (RP0004).",
                },
            ),
            (
                "score",
                {
                    "default": True,
                    "type": "yn",
                    "metavar": "<y or n>",
                    "short": "s",
                    "group": "Reports",
                    "help": "Activate the evaluation score.",
                },
            ),
            (
                "fail-under",
                {
                    "default": 10,
                    "type": "float",
                    "metavar": "<score>",
                    "help": "Specify a score threshold to be exceeded before program exits with error.",
                },
            ),
            (
                "fail-on",
                {
                    "default": "",
                    "type": "csv",
                    "metavar": "<msg ids>",
                    "help": "Return non-zero exit code if any of these messages/categories are detected,"
                    " even if score is above --fail-under value. Syntax same as enable."
                    " Messages specified are enabled, while categories only check already-enabled messages.",
                },
            ),
            (
                "confidence",
                {
                    "type": "multiple_choice",
                    "metavar": "<levels>",
                    "default": "",
                    "choices": [c.name for c in interfaces.CONFIDENCE_LEVELS],
                    "group": "Messages control",
                    "help": "Only show warnings with the listed confidence levels."
                    f" Leave empty to show all. Valid levels: {', '.join(c.name for c in interfaces.CONFIDENCE_LEVELS)}.",
                },
            ),
            (
                "enable",
                {
                    "type": "csv",
                    "metavar": "<msg ids>",
                    "short": "e",
                    "group": "Messages control",
                    "help": "Enable the message, report, category or checker with the "
                    "given id(s). You can either give multiple identifier "
                    "separated by comma (,) or put this option multiple time "
                    "(only on the command line, not in the configuration file "
                    "where it should appear only once). "
                    'See also the "--disable" option for examples.',
                },
            ),
            (
                "disable",
                {
                    "type": "csv",
                    "metavar": "<msg ids>",
                    "short": "d",
                    "group": "Messages control",
                    "help": "Disable the message, report, category or checker "
                    "with the given id(s). You can either give multiple identifiers "
                    "separated by comma (,) or put this option multiple times "
                    "(only on the command line, not in the configuration file "
                    "where it should appear only once). "
                    'You can also use "--disable=all" to disable everything first '
                    "and then reenable specific checks. For example, if you want "
                    "to run only the similarities checker, you can use "
                    '"--disable=all --enable=similarities". '
                    "If you want to run only the classes checker, but have no "
                    "Warning level messages displayed, use "
                    '"--disable=all --enable=classes --disable=W".',
                },
            ),
            (
                "msg-template",
                {
                    "type": "string",
                    "metavar": "<template>",
                    "group": "Reports",
                    "help": (
                        "Template used to display messages. "
                        "This is a python new-style format string "
                        "used to format the message information. "
                        "See doc for all details."
                    ),
                },
            ),
            (
                "jobs",
                {
                    "type": "int",
                    "metavar": "<n-processes>",
                    "short": "j",
                    "default": 1,
                    "help": "Use multiple processes to speed up Pylint. Specifying 0 will "
                    "auto-detect the number of processors available to use.",
                },
            ),
            (
                "unsafe-load-any-extension",
                {
                    "type": "yn",
                    "metavar": "<y or n>",
                    "default": False,
                    "hide": True,
                    "help": (
                        "Allow loading of arbitrary C extensions. Extensions"
                        " are imported into the active Python interpreter and"
                        " may run arbitrary code."
                    ),
                },
            ),
            (
                "limit-inference-results",
                {
                    "type": "int",
                    "metavar": "<number-of-results>",
                    "default": 100,
                    "help": (
                        "Control the amount of potential inferred values when inferring "
                        "a single object. This can help the performance when dealing with "
                        "large functions or complex, nested conditions. "
                    ),
                },
            ),
            (
                "extension-pkg-allow-list",
                {
                    "type": "csv",
                    "metavar": "<pkg[,pkg]>",
                    "default": [],
                    "help": (
                        "A comma-separated list of package or module names"
                        " from where C extensions may be loaded. Extensions are"
                        " loading into the active Python interpreter and may run"
                        " arbitrary code."
                    ),
                },
            ),
            (
                "extension-pkg-whitelist",
                {
                    "type": "csv",
                    "metavar": "<pkg[,pkg]>",
                    "default": [],
                    "help": (
                        "A comma-separated list of package or module names"
                        " from where C extensions may be loaded. Extensions are"
                        " loading into the active Python interpreter and may run"
                        " arbitrary code. (This is an alternative name to"
                        " extension-pkg-allow-list for backward compatibility.)"
                    ),
                },
            ),
            (
                "suggestion-mode",
                {
                    "type": "yn",
                    "metavar": "<y or n>",
                    "default": True,
                    "help": (
                        "When enabled, pylint would attempt to guess common "
                        "misconfiguration and emit user-friendly hints instead "
                        "of false-positive error messages."
                    ),
                },
            ),
            (
                "exit-zero",
                {
                    "action": "store_true",
                    "help": (
                        "Always return a 0 (non-error) status code, even if "
                        "lint errors are found. This is primarily useful in "
                        "continuous integration scripts."
                    ),
                },
            ),
            (
                "from-stdin",
                {
                    "action": "store_true",
                    "help": (
                        "Interpret the stdin as a python script, whose filename "
                        "needs to be passed as the module_or_package argument."
                    ),
                },
            ),
            (
                "py-version",
                {
                    "default": sys.version_info[:2],
                    "type": "py_version",
                    "metavar": "<py_version>",
                    "help": (
                        "Minimum Python version to use for version dependent checks. "
                        "Will default to the version used to run pylint."
                    ),
                },
            ),
        )

    option_groups = (
        ("Messages control", "Options controlling analysis messages"),
        ("Reports", "Options related to output formatting and reporting"),
    )

    def __init__(self, options=(), reporter=None, option_groups=(), pylintrc=None):
        """Some stuff has to be done before ancestors initialization...
        messages store / checkers / reporter / astroid manager"""
        self.msgs_store = MessageDefinitionStore()
        self.reporter = None
        self._reporter_names = None
        self._reporters = {}
        self._checkers = collections.defaultdict(list)
        self._pragma_lineno = {}
        self._ignore_file = False
        # visit variables
        self.file_state = FileState()
        self.current_name: Optional[str] = None
        self.current_file = None
        self.stats = LinterStats()
        self.fail_on_symbols = []
        # init options
        self._external_opts = options
        self.options = options + PyLinter.make_options()
        self.option_groups = option_groups + PyLinter.option_groups
        self._options_methods = {
            "enable": self.enable,
            "disable": self.disable,
            "disable-next": self.disable_next,
        }
        self._bw_options_methods = {
            "disable-msg": self._options_methods["disable"],
            "enable-msg": self._options_methods["enable"],
        }

        # Attributes related to message (state) handling
        self.msg_status = 0
        self._msgs_state: Dict[str, bool] = {}
        self._by_id_managed_msgs: List[ManagedMessage] = []

        reporters.ReportsHandlerMixIn.__init__(self)
        super().__init__(
            usage=__doc__,
            config_file=pylintrc or next(config.find_default_config_files(), None),
        )
        checkers.BaseTokenChecker.__init__(self)
        # provided reports
        self.reports = (
            ("RP0001", "Messages by category", report_total_messages_stats),
            (
                "RP0002",
                "% errors / warnings by module",
                report_messages_by_module_stats,
            ),
            ("RP0003", "Messages", report_messages_stats),
        )
        self.register_checker(self)
        self._dynamic_plugins = set()
        self._error_mode = False
        self.load_provider_defaults()
        if reporter:
            self.set_reporter(reporter)

    def load_default_plugins(self):
        checkers.initialize(self)
        reporters.initialize(self)
        # Make sure to load the default reporter, because
        # the option has been set before the plugins had been loaded.
        if not self.reporter:
            self._load_reporters()

    def load_plugin_modules(self, modnames):
        """take a list of module names which are pylint plugins and load
        and register them
        """
        for modname in modnames:
            if modname in self._dynamic_plugins:
                continue
            self._dynamic_plugins.add(modname)
            try:
                module = astroid.modutils.load_module_from_name(modname)
                module.register(self)
            except ModuleNotFoundError:
                pass

    def load_plugin_configuration(self):
        """Call the configuration hook for plugins

        This walks through the list of plugins, grabs the "load_configuration"
        hook, if exposed, and calls it to allow plugins to configure specific
        settings.
        """
        for modname in self._dynamic_plugins:
            try:
                module = astroid.modutils.load_module_from_name(modname)
                if hasattr(module, "load_configuration"):
                    module.load_configuration(self)
            except ModuleNotFoundError as e:
                self.add_message("bad-plugin-value", args=(modname, e), line=0)

    def _load_reporters(self) -> None:
        sub_reporters = []
        output_files = []
        with contextlib.ExitStack() as stack:
            for reporter_name in self._reporter_names.split(","):
                reporter_name, *reporter_output = reporter_name.split(":", 1)

                reporter = self._load_reporter_by_name(reporter_name)
                sub_reporters.append(reporter)
                if reporter_output:
                    (reporter_output,) = reporter_output
                    output_file = stack.enter_context(
                        open(reporter_output, "w", encoding="utf-8")
                    )
                    reporter.out = output_file
                    output_files.append(output_file)

            # Extend the lifetime of all opened output files
            close_output_files = stack.pop_all().close

        if len(sub_reporters) > 1 or output_files:
            self.set_reporter(
                reporters.MultiReporter(
                    sub_reporters,
                    close_output_files,
                )
            )
        else:
            self.set_reporter(sub_reporters[0])

    def _load_reporter_by_name(self, reporter_name: str) -> reporters.BaseReporter:
        name = reporter_name.lower()
        if name in self._reporters:
            return self._reporters[name]()

        try:
            reporter_class = _load_reporter_by_class(reporter_name)
        except (ImportError, AttributeError) as e:
            raise exceptions.InvalidReporterError(name) from e
        else:
            return reporter_class()

    def set_reporter(self, reporter):
        """set the reporter used to display messages and reports"""
        self.reporter = reporter
        reporter.linter = self

    def set_option(self, optname, value, action=None, optdict=None):
        """overridden from config.OptionsProviderMixin to handle some
        special options
        """
        if optname in self._options_methods or optname in self._bw_options_methods:
            if value:
                try:
                    meth = self._options_methods[optname]
                except KeyError:
                    meth = self._bw_options_methods[optname]
                    warnings.warn(
                        f"{optname} is deprecated, replace it by {optname.split('-')[0]}",
                        DeprecationWarning,
                    )
                value = utils._check_csv(value)
                if isinstance(value, (list, tuple)):
                    for _id in value:
                        meth(_id, ignore_unknown=True)
                else:
                    meth(value)
                return  # no need to call set_option, disable/enable methods do it
        elif optname == "output-format":
            self._reporter_names = value
            # If the reporters are already available, load
            # the reporter class.
            if self._reporters:
                self._load_reporters()

        try:
            checkers.BaseTokenChecker.set_option(self, optname, value, action, optdict)
        except config.UnsupportedAction:
            print(f"option {optname} can't be read from config file", file=sys.stderr)

    def register_reporter(self, reporter_class):
        self._reporters[reporter_class.name] = reporter_class

    def report_order(self):
        reports = sorted(self._reports, key=lambda x: getattr(x, "name", ""))
        try:
            # Remove the current reporter and add it
            # at the end of the list.
            reports.pop(reports.index(self))
        except ValueError:
            pass
        else:
            reports.append(self)
        return reports

    # checkers manipulation methods ############################################

    def register_checker(self, checker):
        """register a new checker

        checker is an object implementing IRawChecker or / and IAstroidChecker
        """
        assert checker.priority <= 0, "checker priority can't be >= 0"
        self._checkers[checker.name].append(checker)
        for r_id, r_title, r_cb in checker.reports:
            self.register_report(r_id, r_title, r_cb, checker)
        self.register_options_provider(checker)
        if hasattr(checker, "msgs"):
            self.msgs_store.register_messages_from_checker(checker)
        checker.load_defaults()

        # Register the checker, but disable all of its messages.
        if not getattr(checker, "enabled", True):
            self.disable(checker.name)

    def enable_fail_on_messages(self):
        """enable 'fail on' msgs

        Convert values in config.fail_on (which might be msg category, msg id,
        or symbol) to specific msgs, then enable and flag them for later.
        """
        fail_on_vals = self.config.fail_on
        if not fail_on_vals:
            return

        fail_on_cats = set()
        fail_on_msgs = set()
        for val in fail_on_vals:
            # If value is a cateogry, add category, else add message
            if val in MSG_TYPES:
                fail_on_cats.add(val)
            else:
                fail_on_msgs.add(val)

        # For every message in every checker, if cat or msg flagged, enable check
        for all_checkers in self._checkers.values():
            for checker in all_checkers:
                for msg in checker.messages:
                    if msg.msgid in fail_on_msgs or msg.symbol in fail_on_msgs:
                        # message id/symbol matched, enable and flag it
                        self.enable(msg.msgid)
                        self.fail_on_symbols.append(msg.symbol)
                    elif msg.msgid[0] in fail_on_cats:
                        # message starts with a cateogry value, flag (but do not enable) it
                        self.fail_on_symbols.append(msg.symbol)

    def any_fail_on_issues(self):
        return self.stats and any(
            x in self.fail_on_symbols for x in self.stats.by_msg.keys()
        )

    def disable_noerror_messages(self):
        for msgcat, msgids in self.msgs_store._msgs_by_category.items():
            # enable only messages with 'error' severity and above ('fatal')
            if msgcat in {"E", "F"}:
                for msgid in msgids:
                    self.enable(msgid)
            else:
                for msgid in msgids:
                    self.disable(msgid)

    def disable_reporters(self):
        """disable all reporters"""
        for _reporters in self._reports.values():
            for report_id, _, _ in _reporters:
                self.disable_report(report_id)

    def error_mode(self):
        """error mode: enable only errors; no reports, no persistent"""
        self._error_mode = True
        self.disable_noerror_messages()
        self.disable("miscellaneous")
        self.set_option("reports", False)
        self.set_option("persistent", False)
        self.set_option("score", False)

    def list_messages_enabled(self):
        emittable, non_emittable = self.msgs_store.find_emittable_messages()
        enabled = []
        disabled = []
        for message in emittable:
            if self.is_message_enabled(message.msgid):
                enabled.append(f"  {message.symbol} ({message.msgid})")
            else:
                disabled.append(f"  {message.symbol} ({message.msgid})")
        print("Enabled messages:")
        for msg in enabled:
            print(msg)
        print("\nDisabled messages:")
        for msg in disabled:
            print(msg)
        print("\nNon-emittable messages with current interpreter:")
        for msg in non_emittable:
            print(f"  {msg.symbol} ({msg.msgid})")
        print("")

    # block level option handling #############################################
    # see func_block_disable_msg.py test case for expected behaviour

    def process_tokens(self, tokens):
        """Process tokens from the current module to search for module/block level
        options."""
        control_pragmas = {"disable", "disable-next", "enable"}
        prev_line = None
        saw_newline = True
        seen_newline = True
        for (tok_type, content, start, _, _) in tokens:
            if prev_line and prev_line != start[0]:
                saw_newline = seen_newline
                seen_newline = False

            prev_line = start[0]
            if tok_type in (tokenize.NL, tokenize.NEWLINE):
                seen_newline = True

            if tok_type != tokenize.COMMENT:
                continue
            match = OPTION_PO.search(content)
            if match is None:
                continue
            try:
                for pragma_repr in parse_pragma(match.group(2)):
                    if pragma_repr.action in {"disable-all", "skip-file"}:
                        if pragma_repr.action == "disable-all":
                            self.add_message(
                                "deprecated-pragma",
                                line=start[0],
                                args=("disable-all", "skip-file"),
                            )
                        self.add_message("file-ignored", line=start[0])
                        self._ignore_file = True
                        return
                    try:
                        meth = self._options_methods[pragma_repr.action]
                    except KeyError:
                        meth = self._bw_options_methods[pragma_repr.action]
                        # found a "(dis|en)able-msg" pragma deprecated suppression
                        self.add_message(
                            "deprecated-pragma",
                            line=start[0],
                            args=(
                                pragma_repr.action,
                                pragma_repr.action.replace("-msg", ""),
                            ),
                        )
                    for msgid in pragma_repr.messages:
                        # Add the line where a control pragma was encountered.
                        if pragma_repr.action in control_pragmas:
                            self._pragma_lineno[msgid] = start[0]

                        if (pragma_repr.action, msgid) == ("disable", "all"):
                            self.add_message(
                                "deprecated-pragma",
                                line=start[0],
                                args=("disable=all", "skip-file"),
                            )
                            self.add_message("file-ignored", line=start[0])
                            self._ignore_file = True
                            return
                            # If we did not see a newline between the previous line and now,
                            # we saw a backslash so treat the two lines as one.
                        l_start = start[0]
                        if not saw_newline:
                            l_start -= 1
                        try:
                            meth(msgid, "module", l_start)
                        except exceptions.UnknownMessageError:
                            self.add_message(
                                "bad-option-value", args=msgid, line=start[0]
                            )
            except UnRecognizedOptionError as err:
                self.add_message(
                    "unrecognized-inline-option", args=err.token, line=start[0]
                )
                continue
            except InvalidPragmaError as err:
                self.add_message("bad-inline-option", args=err.token, line=start[0])
                continue

    # code checking methods ###################################################

    def get_checkers(self):
        """return all available checkers as a list"""
        return [self] + [
            c
            for _checkers in self._checkers.values()
            for c in _checkers
            if c is not self
        ]

    def get_checker_names(self):
        """Get all the checker names that this linter knows about."""
        current_checkers = self.get_checkers()
        return sorted(
            {
                checker.name
                for checker in current_checkers
                if checker.name != MAIN_CHECKER_NAME
            }
        )

    def prepare_checkers(self):
        """return checkers needed for activated messages and reports"""
        if not self.config.reports:
            self.disable_reporters()
        # get needed checkers
        needed_checkers = [self]
        for checker in self.get_checkers()[1:]:
            messages = {msg for msg in checker.msgs if self.is_message_enabled(msg)}
            if messages or any(self.report_is_enabled(r[0]) for r in checker.reports):
                needed_checkers.append(checker)
        # Sort checkers by priority
        needed_checkers = sorted(
            needed_checkers, key=operator.attrgetter("priority"), reverse=True
        )
        return needed_checkers

    # pylint: disable=unused-argument
    @staticmethod
    def should_analyze_file(modname, path, is_argument=False):
        """Returns whether or not a module should be checked.

        This implementation returns True for all python source file, indicating
        that all files should be linted.

        Subclasses may override this method to indicate that modules satisfying
        certain conditions should not be linted.

        :param str modname: The name of the module to be checked.
        :param str path: The full path to the source code of the module.
        :param bool is_argument: Whether the file is an argument to pylint or not.
                                 Files which respect this property are always
                                 checked, since the user requested it explicitly.
        :returns: True if the module should be checked.
        :rtype: bool
        """
        if is_argument:
            return True
        return path.endswith(".py")

    # pylint: enable=unused-argument

    def initialize(self):
        """Initialize linter for linting

        This method is called before any linting is done.
        """
        # initialize msgs_state now that all messages have been registered into
        # the store
        for msg in self.msgs_store.messages:
            if not msg.may_be_emitted():
                self._msgs_state[msg.msgid] = False

    def check(self, files_or_modules: Union[Sequence[str], str]) -> None:
        """main checking entry: check a list of files or modules from their name.

        files_or_modules is either a string or list of strings presenting modules to check.
        """
        self.initialize()
        if not isinstance(files_or_modules, (list, tuple)):
            # pylint: disable-next=fixme
            # TODO: Update typing and docstring for 'files_or_modules' when removing the deprecation
            warnings.warn(
                "In pylint 3.0, the checkers check function will only accept sequence of string",
                DeprecationWarning,
            )
            files_or_modules = (files_or_modules,)  # type: ignore[assignment]
        if self.config.from_stdin:
            if len(files_or_modules) != 1:
                raise exceptions.InvalidArgsError(
                    "Missing filename required for --from-stdin"
                )

            filepath = files_or_modules[0]
            with fix_import_path(files_or_modules):
                self._check_files(
                    functools.partial(self.get_ast, data=_read_stdin()),
                    [self._get_file_descr_from_stdin(filepath)],
                )
        elif self.config.jobs == 1:
            with fix_import_path(files_or_modules):
                self._check_files(
                    self.get_ast, self._iterate_file_descrs(files_or_modules)
                )
        else:
            check_parallel(
                self,
                self.config.jobs,
                self._iterate_file_descrs(files_or_modules),
                files_or_modules,
            )

    def check_single_file(self, name: str, filepath: str, modname: str) -> None:
        warnings.warn(
            "In pylint 3.0, the checkers check_single_file function will be removed. "
            "Use check_single_file_item instead.",
            DeprecationWarning,
        )
        self.check_single_file_item(FileItem(name, filepath, modname))

    def check_single_file_item(self, file: FileItem) -> None:
        """Check single file item

        The arguments are the same that are documented in _check_files

        The initialize() method should be called before calling this method
        """
        with self._astroid_module_checker() as check_astroid_module:
            self._check_file(self.get_ast, check_astroid_module, file)

    def _check_files(
        self,
        get_ast,
        file_descrs: Iterable[FileItem],
    ) -> None:
        """Check all files from file_descrs"""
        with self._astroid_module_checker() as check_astroid_module:
            for file in file_descrs:
                try:
                    self._check_file(get_ast, check_astroid_module, file)
                except Exception as ex:  # pylint: disable=broad-except
                    template_path = prepare_crash_report(
                        ex, file.filepath, self.crash_file_path
                    )
                    msg = get_fatal_error_message(file.filepath, template_path)
                    if isinstance(ex, AstroidError):
                        symbol = "astroid-error"
                        self.add_message(symbol, args=(file.filepath, msg))
                    else:
                        symbol = "fatal"
                        self.add_message(symbol, args=msg)

    def _check_file(self, get_ast, check_astroid_module, file: FileItem):
        """Check a file using the passed utility functions (get_ast and check_astroid_module)

        :param callable get_ast: callable returning AST from defined file taking the following arguments
        - filepath: path to the file to check
        - name: Python module name
        :param callable check_astroid_module: callable checking an AST taking the following arguments
        - ast: AST of the module
        :param FileItem file: data about the file
        """
        self.set_current_module(file.name, file.filepath)
        # get the module representation
        ast_node = get_ast(file.filepath, file.name)
        if ast_node is None:
            return

        self._ignore_file = False

        self.file_state = FileState(file.modpath)
        # fix the current file (if the source file was not available or
        # if it's actually a c extension)
        self.current_file = ast_node.file
        check_astroid_module(ast_node)
        # warn about spurious inline messages handling
        spurious_messages = self.file_state.iter_spurious_suppression_messages(
            self.msgs_store
        )
        for msgid, line, args in spurious_messages:
            self.add_message(msgid, line, None, args)

    @staticmethod
    def _get_file_descr_from_stdin(filepath: str) -> FileItem:
        """Return file description (tuple of module name, file path, base name) from given file path

        This method is used for creating suitable file description for _check_files when the
        source is standard input.
        """
        try:
            # Note that this function does not really perform an
            # __import__ but may raise an ImportError exception, which
            # we want to catch here.
            modname = ".".join(astroid.modutils.modpath_from_file(filepath))
        except ImportError:
            modname = os.path.splitext(os.path.basename(filepath))[0]

        return FileItem(modname, filepath, filepath)

    def _iterate_file_descrs(self, files_or_modules) -> Iterator[FileItem]:
        """Return generator yielding file descriptions (tuples of module name, file path, base name)

        The returned generator yield one item for each Python module that should be linted.
        """
        for descr in self._expand_files(files_or_modules):
            name, filepath, is_arg = descr["name"], descr["path"], descr["isarg"]
            if self.should_analyze_file(name, filepath, is_argument=is_arg):
                yield FileItem(name, filepath, descr["basename"])

    def _expand_files(self, modules) -> List[ModuleDescriptionDict]:
        """get modules and errors from a list of modules and handle errors"""
        result, errors = expand_modules(
            modules,
            self.config.black_list,
            self.config.black_list_re,
            self._ignore_paths,
        )
        for error in errors:
            message = modname = error["mod"]
            key = error["key"]
            self.set_current_module(modname)
            if key == "fatal":
                message = str(error["ex"]).replace(os.getcwd() + os.sep, "")
            self.add_message(key, args=message)
        return result

    def set_current_module(self, modname, filepath: Optional[str] = None):
        """set the name of the currently analyzed module and
        init statistics for it
        """
        if not modname and filepath is None:
            return
        self.reporter.on_set_current_module(modname, filepath)
        self.current_name = modname
        self.current_file = filepath or modname
        self.stats.init_single_module(modname)

    @contextlib.contextmanager
    def _astroid_module_checker(self):
        """Context manager for checking ASTs

        The value in the context is callable accepting AST as its only argument.
        """
        walker = ASTWalker(self)
        _checkers = self.prepare_checkers()
        tokencheckers = [
            c
            for c in _checkers
            if interfaces.implements(c, interfaces.ITokenChecker) and c is not self
        ]
        rawcheckers = [
            c for c in _checkers if interfaces.implements(c, interfaces.IRawChecker)
        ]
        # notify global begin
        for checker in _checkers:
            checker.open()
            if interfaces.implements(checker, interfaces.IAstroidChecker):
                walker.add_checker(checker)

        yield functools.partial(
            self.check_astroid_module,
            walker=walker,
            tokencheckers=tokencheckers,
            rawcheckers=rawcheckers,
        )

        # notify global end
        self.stats.statement = walker.nbstatements
        for checker in reversed(_checkers):
            checker.close()

    def get_ast(self, filepath, modname, data=None):
        """Return an ast(roid) representation of a module or a string.

        :param str filepath: path to checked file.
        :param str modname: The name of the module to be checked.
        :param str data: optional contents of the checked file.
        :returns: the AST
        :rtype: astroid.nodes.Module
        """
        try:
            if data is None:
                return MANAGER.ast_from_file(filepath, modname, source=True)
            return astroid.builder.AstroidBuilder(MANAGER).string_build(
                data, modname, filepath
            )
        except astroid.AstroidSyntaxError as ex:
            # pylint: disable=no-member
            self.add_message(
                "syntax-error",
                line=getattr(ex.error, "lineno", 0),
                col_offset=getattr(ex.error, "offset", None),
                args=str(ex.error),
            )
        except astroid.AstroidBuildingException as ex:
            self.add_message("parse-error", args=ex)
        except Exception as ex:  # pylint: disable=broad-except
            traceback.print_exc()
            self.add_message("astroid-error", args=(ex.__class__, ex))
        return None

    def check_astroid_module(self, ast_node, walker, rawcheckers, tokencheckers):
        """Check a module from its astroid representation.

        For return value see _check_astroid_module
        """
        before_check_statements = walker.nbstatements

        retval = self._check_astroid_module(
            ast_node, walker, rawcheckers, tokencheckers
        )

        self.stats.by_module[self.current_name]["statement"] = (
            walker.nbstatements - before_check_statements
        )

        return retval

    def _check_astroid_module(
        self, node: nodes.Module, walker, rawcheckers, tokencheckers
    ):
        """Check given AST node with given walker and checkers

        :param astroid.nodes.Module node: AST node of the module to check
        :param pylint.utils.ast_walker.ASTWalker walker: AST walker
        :param list rawcheckers: List of token checkers to use
        :param list tokencheckers: List of raw checkers to use

        :returns: True if the module was checked, False if ignored,
            None if the module contents could not be parsed
        :rtype: bool
        """
        try:
            tokens = utils.tokenize_module(node)
        except tokenize.TokenError as ex:
            self.add_message("syntax-error", line=ex.args[1][0], args=ex.args[0])
            return None

        if not node.pure_python:
            self.add_message("raw-checker-failed", args=node.name)
        else:
            # assert astroid.file.endswith('.py')
            # invoke ITokenChecker interface on self to fetch module/block
            # level options
            self.process_tokens(tokens)
            if self._ignore_file:
                return False
            # walk ast to collect line numbers
            self.file_state.collect_block_lines(self.msgs_store, node)
            # run raw and tokens checkers
            for checker in rawcheckers:
                checker.process_module(node)
            for checker in tokencheckers:
                checker.process_tokens(tokens)
        # generate events to astroid checkers
        walker.walk(node)
        return True

    # IAstroidChecker interface #################################################

    def open(self):
        """initialize counters"""
        self.stats = LinterStats()
        MANAGER.always_load_extensions = self.config.unsafe_load_any_extension
        MANAGER.max_inferable_values = self.config.limit_inference_results
        MANAGER.extension_package_whitelist.update(self.config.extension_pkg_allow_list)
        if self.config.extension_pkg_whitelist:
            MANAGER.extension_package_whitelist.update(
                self.config.extension_pkg_whitelist
            )
        self.stats.reset_message_count()
        self._ignore_paths = get_global_option(self, "ignore-paths")

    def generate_reports(self):
        """close the whole package /module, it's time to make reports !

        if persistent run, pickle results for later comparison
        """
        # Display whatever messages are left on the reporter.
        self.reporter.display_messages(report_nodes.Section())

        if self.file_state.base_name is not None:
            # load previous results if any
            previous_stats = config.load_results(self.file_state.base_name)
            self.reporter.on_close(self.stats, previous_stats)
            if self.config.reports:
                sect = self.make_reports(self.stats, previous_stats)
            else:
                sect = report_nodes.Section()

            if self.config.reports:
                self.reporter.display_reports(sect)
            score_value = self._report_evaluation()
            # save results if persistent run
            if self.config.persistent:
                config.save_results(self.stats, self.file_state.base_name)
        else:
            self.reporter.on_close(self.stats, LinterStats())
            score_value = None
        return score_value

    def _report_evaluation(self):
        """make the global evaluation report"""
        # check with at least check 1 statements (usually 0 when there is a
        # syntax error preventing pylint from further processing)
        note = None
        previous_stats = config.load_results(self.file_state.base_name)
        if self.stats.statement == 0:
            return note

        # get a global note for the code
        evaluation = self.config.evaluation
        try:
            stats_dict = {
                "error": self.stats.error,
                "warning": self.stats.warning,
                "refactor": self.stats.refactor,
                "convention": self.stats.convention,
                "statement": self.stats.statement,
                "info": self.stats.info,
            }
            note = eval(evaluation, {}, stats_dict)  # pylint: disable=eval-used
        except Exception as ex:  # pylint: disable=broad-except
            msg = f"An exception occurred while rating: {ex}"
        else:
            self.stats.global_note = note
            msg = f"Your code has been rated at {note:.2f}/10"
            if previous_stats:
                pnote = previous_stats.global_note
                if pnote is not None:
                    msg += f" (previous run: {pnote:.2f}/10, {note - pnote:+.2f})"

        if self.config.score:
            sect = report_nodes.EvaluationSection(msg)
            self.reporter.display_reports(sect)
        return note

    # Adding (ignored) messages to the Message Reporter

    def _get_message_state_scope(
        self,
        msgid: str,
        line: Optional[int] = None,
        confidence: Optional[interfaces.Confidence] = None,
    ) -> Optional[Literal[0, 1, 2]]:
        """Returns the scope at which a message was enabled/disabled."""
        if confidence is None:
            confidence = interfaces.UNDEFINED
        if self.config.confidence and confidence.name not in self.config.confidence:
            return MSG_STATE_CONFIDENCE  # type: ignore[return-value] # mypy does not infer Literal correctly
        try:
            if line in self.file_state._module_msgs_state[msgid]:
                return MSG_STATE_SCOPE_MODULE  # type: ignore[return-value]
        except (KeyError, TypeError):
            return MSG_STATE_SCOPE_CONFIG  # type: ignore[return-value]
        return None

    def _is_one_message_enabled(self, msgid: str, line: Optional[int]) -> bool:
        """Checks state of a single message"""
        if line is None:
            return self._msgs_state.get(msgid, True)
        try:
            return self.file_state._module_msgs_state[msgid][line]
        except KeyError:
            # Check if the message's line is after the maximum line existing in ast tree.
            # This line won't appear in the ast tree and won't be referred in
            # self.file_state._module_msgs_state
            # This happens for example with a commented line at the end of a module.
            max_line_number = self.file_state.get_effective_max_line_number()
            if max_line_number and line > max_line_number:
                fallback = True
                lines = self.file_state._raw_module_msgs_state.get(msgid, {})

                # Doesn't consider scopes, as a disable can be in a different scope
                # than that of the current line.
                closest_lines = reversed(
                    [
                        (message_line, enable)
                        for message_line, enable in lines.items()
                        if message_line <= line
                    ]
                )
                _, fallback_iter = next(closest_lines, (None, None))
                if fallback_iter is not None:
                    fallback = fallback_iter

                return self._msgs_state.get(msgid, fallback)
            return self._msgs_state.get(msgid, True)

    def is_message_enabled(
        self,
        msg_descr: str,
        line: Optional[int] = None,
        confidence: Optional[interfaces.Confidence] = None,
    ) -> bool:
        """return whether the message associated to the given message id is
        enabled

        msgid may be either a numeric or symbolic message id.
        """
        if self.config.confidence and confidence:
            if confidence.name not in self.config.confidence:
                return False
        try:
            message_definitions = self.msgs_store.get_message_definitions(msg_descr)
            msgids = [md.msgid for md in message_definitions]
        except exceptions.UnknownMessageError:
            # The linter checks for messages that are not registered
            # due to version mismatch, just treat them as message IDs
            # for now.
            msgids = [msg_descr]
        return any(self._is_one_message_enabled(msgid, line) for msgid in msgids)

    def _add_one_message(
        self,
        message_definition: MessageDefinition,
        line: Optional[int],
        node: Optional[nodes.NodeNG],
        args: Optional[Any],
        confidence: Optional[interfaces.Confidence],
        col_offset: Optional[int],
        end_lineno: Optional[int],
        end_col_offset: Optional[int],
    ) -> None:
        """After various checks have passed a single Message is
        passed to the reporter and added to stats"""
        message_definition.check_message_definition(line, node)

        # Look up "location" data of node if not yet supplied
        if node:
            if not line:
                line = node.fromlineno
            # pylint: disable=fixme
            # TODO: Initialize col_offset on every node (can be None) -> astroid
            if not col_offset and hasattr(node, "col_offset"):
                col_offset = node.col_offset
            # pylint: disable=fixme
            # TODO: Initialize end_lineno on every node (can be None) -> astroid
            # See https://github.com/PyCQA/astroid/issues/1273
            if not end_lineno and hasattr(node, "end_lineno"):
                end_lineno = node.end_lineno
            # pylint: disable=fixme
            # TODO: Initialize end_col_offset on every node (can be None) -> astroid
            if not end_col_offset and hasattr(node, "end_col_offset"):
                end_col_offset = node.end_col_offset

        # should this message be displayed
        if not self.is_message_enabled(message_definition.msgid, line, confidence):
            self.file_state.handle_ignored_message(
                self._get_message_state_scope(
                    message_definition.msgid, line, confidence
                ),
                message_definition.msgid,
                line,
            )
            return

        # update stats
        msg_cat = MSG_TYPES[message_definition.msgid[0]]
        self.msg_status |= MSG_TYPES_STATUS[message_definition.msgid[0]]
        self.stats.increase_single_message_count(msg_cat, 1)
        self.stats.increase_single_module_message_count(self.current_name, msg_cat, 1)
        try:
            self.stats.by_msg[message_definition.symbol] += 1
        except KeyError:
            self.stats.by_msg[message_definition.symbol] = 1
        # Interpolate arguments into message string
        msg = message_definition.msg
        if args:
            msg %= args
        # get module and object
        if node is None:
            module, obj = self.current_name, ""
            abspath = self.current_file
        else:
            module, obj = utils.get_module_and_frameid(node)
            abspath = node.root().file
        if abspath is not None:
            path = abspath.replace(self.reporter.path_strip_prefix, "", 1)
        else:
            path = "configuration"
        # add the message
        self.reporter.handle_message(
            Message(
                message_definition.msgid,
                message_definition.symbol,
                MessageLocationTuple(
                    abspath,
                    path,
                    module or "",
                    obj,
                    line or 1,
                    col_offset or 0,
                    end_lineno,
                    end_col_offset,
                ),
                msg,
                confidence,
            )
        )

    def add_message(
        self,
        msgid: str,
        line: Optional[int] = None,
        node: Optional[nodes.NodeNG] = None,
        args: Optional[Any] = None,
        confidence: Optional[interfaces.Confidence] = None,
        col_offset: Optional[int] = None,
        end_lineno: Optional[int] = None,
        end_col_offset: Optional[int] = None,
    ) -> None:
        """Adds a message given by ID or name.

        If provided, the message string is expanded using args.

        AST checkers must provide the node argument (but may optionally
        provide line if the line number is different), raw and token checkers
        must provide the line argument.
        """
        if confidence is None:
            confidence = interfaces.UNDEFINED
        message_definitions = self.msgs_store.get_message_definitions(msgid)
        for message_definition in message_definitions:
            self._add_one_message(
                message_definition,
                line,
                node,
                args,
                confidence,
                col_offset,
                end_lineno,
                end_col_offset,
            )

    def add_ignored_message(
        self,
        msgid: str,
        line: int,
        node: Optional[nodes.NodeNG] = None,
        confidence: Optional[interfaces.Confidence] = interfaces.UNDEFINED,
    ) -> None:
        """Prepares a message to be added to the ignored message storage

        Some checks return early in special cases and never reach add_message(),
        even though they would normally issue a message.
        This creates false positives for useless-suppression.
        This function avoids this by adding those message to the ignored msgs attribute
        """
        message_definitions = self.msgs_store.get_message_definitions(msgid)
        for message_definition in message_definitions:
            message_definition.check_message_definition(line, node)
            self.file_state.handle_ignored_message(
                self._get_message_state_scope(
                    message_definition.msgid, line, confidence
                ),
                message_definition.msgid,
                line,
            )

    # Setting the state (disabled/enabled) of messages and registering them

    def _message_symbol(self, msgid: str) -> List[str]:
        """Get the message symbol of the given message id

        Return the original message id if the message does not
        exist.
        """
        try:
            return [md.symbol for md in self.msgs_store.get_message_definitions(msgid)]
        except exceptions.UnknownMessageError:
            return [msgid]

    def _set_one_msg_status(
        self, scope: str, msg: MessageDefinition, line: Optional[int], enable: bool
    ) -> None:
        """Set the status of an individual message"""
        if scope == "module":
            self.file_state.set_msg_status(msg, line, enable)
            if not enable and msg.symbol != "locally-disabled":
                self.add_message(
                    "locally-disabled", line=line, args=(msg.symbol, msg.msgid)
                )
        else:
            msgs = self._msgs_state
            msgs[msg.msgid] = enable
            # sync configuration object
            self.config.enable = [
                self._message_symbol(mid) for mid, val in sorted(msgs.items()) if val
            ]
            self.config.disable = [
                self._message_symbol(mid)
                for mid, val in sorted(msgs.items())
                if not val
            ]

    def _set_msg_status(
        self,
        msgid: str,
        enable: bool,
        scope: str = "package",
        line: Optional[int] = None,
        ignore_unknown: bool = False,
    ) -> None:
        """Do some tests and then iterate over message defintions to set state"""
        assert scope in {"package", "module"}
        if msgid == "all":
            for _msgid in MSG_TYPES:
                self._set_msg_status(_msgid, enable, scope, line, ignore_unknown)
            return

        # msgid is a category?
        category_id = msgid.upper()
        if category_id not in MSG_TYPES:
            category_id_formatted = MSG_TYPES_LONG.get(category_id)
        else:
            category_id_formatted = category_id
        if category_id_formatted is not None:
            for _msgid in self.msgs_store._msgs_by_category.get(category_id_formatted):
                self._set_msg_status(_msgid, enable, scope, line)
            return

        # msgid is a checker name?
        if msgid.lower() in self._checkers:
            for checker in self._checkers[msgid.lower()]:
                for _msgid in checker.msgs:
                    self._set_msg_status(_msgid, enable, scope, line)
            return

        # msgid is report id?
        if msgid.lower().startswith("rp"):
            if enable:
                self.enable_report(msgid)
            else:
                self.disable_report(msgid)
            return

        try:
            # msgid is a symbolic or numeric msgid.
            message_definitions = self.msgs_store.get_message_definitions(msgid)
        except exceptions.UnknownMessageError:
            if ignore_unknown:
                return
            raise
        for message_definition in message_definitions:
            self._set_one_msg_status(scope, message_definition, line, enable)

    def _register_by_id_managed_msg(
        self, msgid_or_symbol: str, line: Optional[int], is_disabled: bool = True
    ) -> None:
        """If the msgid is a numeric one, then register it to inform the user
        it could furnish instead a symbolic msgid."""
        if msgid_or_symbol[1:].isdigit():
            try:
                symbol = self.msgs_store.message_id_store.get_symbol(
                    msgid=msgid_or_symbol
                )
            except exceptions.UnknownMessageError:
                return
            managed = ManagedMessage(
                self.current_name, msgid_or_symbol, symbol, line, is_disabled
            )
            self._by_id_managed_msgs.append(managed)

    def disable(
        self,
        msgid: str,
        scope: str = "package",
        line: Optional[int] = None,
        ignore_unknown: bool = False,
    ) -> None:
        """Disable a message for a scope"""
        self._set_msg_status(
            msgid, enable=False, scope=scope, line=line, ignore_unknown=ignore_unknown
        )
        self._register_by_id_managed_msg(msgid, line)

    def disable_next(
        self,
        msgid: str,
        scope: str = "package",
        line: Optional[int] = None,
        ignore_unknown: bool = False,
    ) -> None:
        """Disable a message for the next line"""
        if not line:
            raise exceptions.NoLineSuppliedError
        self._set_msg_status(
            msgid,
            enable=False,
            scope=scope,
            line=line + 1,
            ignore_unknown=ignore_unknown,
        )
        self._register_by_id_managed_msg(msgid, line + 1)

    def enable(
        self,
        msgid: str,
        scope: str = "package",
        line: Optional[int] = None,
        ignore_unknown: bool = False,
    ) -> None:
        """Enable a message for a scope"""
        self._set_msg_status(
            msgid, enable=True, scope=scope, line=line, ignore_unknown=ignore_unknown
        )
        self._register_by_id_managed_msg(msgid, line, is_disabled=False)
