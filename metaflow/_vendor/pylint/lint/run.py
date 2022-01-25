# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

import os
import sys
import warnings

from metaflow._vendor.pylint import __pkginfo__, extensions, interfaces
from metaflow._vendor.pylint.constants import DEFAULT_PYLINT_HOME, OLD_DEFAULT_PYLINT_HOME, full_version
from metaflow._vendor.pylint.lint.pylinter import PyLinter
from metaflow._vendor.pylint.lint.utils import ArgumentPreprocessingError, preprocess_options
from metaflow._vendor.pylint.utils import print_full_documentation, utils

try:
    import multiprocessing
    from multiprocessing import synchronize  # noqa pylint: disable=unused-import
except ImportError:
    multiprocessing = None  # type: ignore[assignment]


def _cpu_count() -> int:
    """Use sched_affinity if available for virtualized or containerized environments."""
    sched_getaffinity = getattr(os, "sched_getaffinity", None)
    # pylint: disable=not-callable,using-constant-test,useless-suppression
    if sched_getaffinity:
        return len(sched_getaffinity(0))
    if multiprocessing:
        return multiprocessing.cpu_count()
    return 1


def cb_list_extensions(option, optname, value, parser):
    """List all the extensions under pylint.extensions"""

    for filename in os.listdir(os.path.dirname(extensions.__file__)):
        if filename.endswith(".py") and not filename.startswith("_"):
            extension_name, _, _ = filename.partition(".")
            print(f"pylint.extensions.{extension_name}")
    sys.exit(0)


def cb_list_confidence_levels(option, optname, value, parser):
    for level in interfaces.CONFIDENCE_LEVELS:
        print(f"%-18s: {level}")
    sys.exit(0)


def cb_init_hook(optname, value):
    """exec arbitrary code to set sys.path for instance"""
    exec(value)  # pylint: disable=exec-used


UNUSED_PARAM_SENTINEL = object()


class Run:
    """helper class to use as main for pylint :

    run(*sys.argv[1:])
    """

    LinterClass = PyLinter
    option_groups = (
        (
            "Commands",
            "Options which are actually commands. Options in this \
group are mutually exclusive.",
        ),
    )

    @staticmethod
    def _return_one(*args):  # pylint: disable=unused-argument
        return 1

    def __init__(
        self,
        args,
        reporter=None,
        exit=True,
        do_exit=UNUSED_PARAM_SENTINEL,
    ):  # pylint: disable=redefined-builtin
        self._rcfile = None
        self._output = None
        self._version_asked = False
        self._plugins = []
        self.verbose = None
        try:
            preprocess_options(
                args,
                {
                    # option: (callback, takearg)
                    "version": (self.version_asked, False),
                    "init-hook": (cb_init_hook, True),
                    "rcfile": (self.cb_set_rcfile, True),
                    "load-plugins": (self.cb_add_plugins, True),
                    "enable-all-extensions": (self.cb_enable_all_extensions, False),
                    "verbose": (self.cb_verbose_mode, False),
                    "output": (self.cb_set_output, True),
                },
            )
        except ArgumentPreprocessingError as ex:
            print(ex, file=sys.stderr)
            sys.exit(32)

        self.linter = linter = self.LinterClass(
            (
                (
                    "rcfile",
                    {
                        "action": "callback",
                        "callback": Run._return_one,
                        "group": "Commands",
                        "type": "string",
                        "metavar": "<file>",
                        "help": "Specify a configuration file to load.",
                    },
                ),
                (
                    "output",
                    {
                        "action": "callback",
                        "callback": Run._return_one,
                        "group": "Commands",
                        "type": "string",
                        "metavar": "<file>",
                        "help": "Specify an output file.",
                    },
                ),
                (
                    "init-hook",
                    {
                        "action": "callback",
                        "callback": Run._return_one,
                        "type": "string",
                        "metavar": "<code>",
                        "level": 1,
                        "help": "Python code to execute, usually for sys.path "
                        "manipulation such as pygtk.require().",
                    },
                ),
                (
                    "help-msg",
                    {
                        "action": "callback",
                        "type": "string",
                        "metavar": "<msg-id>",
                        "callback": self.cb_help_message,
                        "group": "Commands",
                        "help": "Display a help message for the given message id and "
                        "exit. The value may be a comma separated list of message ids.",
                    },
                ),
                (
                    "list-msgs",
                    {
                        "action": "callback",
                        "metavar": "<msg-id>",
                        "callback": self.cb_list_messages,
                        "group": "Commands",
                        "level": 1,
                        "help": "Display a list of all pylint's messages divided by whether "
                        "they are emittable with the given interpreter.",
                    },
                ),
                (
                    "list-msgs-enabled",
                    {
                        "action": "callback",
                        "metavar": "<msg-id>",
                        "callback": self.cb_list_messages_enabled,
                        "group": "Commands",
                        "level": 1,
                        "help": "Display a list of what messages are enabled, "
                        "disabled and non-emittable with the given configuration.",
                    },
                ),
                (
                    "list-groups",
                    {
                        "action": "callback",
                        "metavar": "<msg-id>",
                        "callback": self.cb_list_groups,
                        "group": "Commands",
                        "level": 1,
                        "help": "List pylint's message groups.",
                    },
                ),
                (
                    "list-conf-levels",
                    {
                        "action": "callback",
                        "callback": cb_list_confidence_levels,
                        "group": "Commands",
                        "level": 1,
                        "help": "Generate pylint's confidence levels.",
                    },
                ),
                (
                    "list-extensions",
                    {
                        "action": "callback",
                        "callback": cb_list_extensions,
                        "group": "Commands",
                        "level": 1,
                        "help": "List available extensions.",
                    },
                ),
                (
                    "full-documentation",
                    {
                        "action": "callback",
                        "metavar": "<msg-id>",
                        "callback": self.cb_full_documentation,
                        "group": "Commands",
                        "level": 1,
                        "help": "Generate pylint's full documentation.",
                    },
                ),
                (
                    "generate-rcfile",
                    {
                        "action": "callback",
                        "callback": self.cb_generate_config,
                        "group": "Commands",
                        "help": "Generate a sample configuration file according to "
                        "the current configuration. You can put other options "
                        "before this one to get them in the generated "
                        "configuration.",
                    },
                ),
                (
                    "generate-man",
                    {
                        "action": "callback",
                        "callback": self.cb_generate_manpage,
                        "group": "Commands",
                        "help": "Generate pylint's man page.",
                        "hide": True,
                    },
                ),
                (
                    "errors-only",
                    {
                        "action": "callback",
                        "callback": self.cb_error_mode,
                        "short": "E",
                        "help": "In error mode, checkers without error messages are "
                        "disabled and for others, only the ERROR messages are "
                        "displayed, and no reports are done by default.",
                    },
                ),
                (
                    "verbose",
                    {
                        "action": "callback",
                        "callback": self.cb_verbose_mode,
                        "short": "v",
                        "help": "In verbose mode, extra non-checker-related info "
                        "will be displayed.",
                    },
                ),
                (
                    "enable-all-extensions",
                    {
                        "action": "callback",
                        "callback": self.cb_enable_all_extensions,
                        "help": "Load and enable all available extensions. "
                        "Use --list-extensions to see a list all available extensions.",
                    },
                ),
            ),
            option_groups=self.option_groups,
            pylintrc=self._rcfile,
        )
        # register standard checkers
        if self._version_asked:
            print(full_version)
            sys.exit(0)
        linter.load_default_plugins()
        # load command line plugins
        linter.load_plugin_modules(self._plugins)
        # add some help section
        linter.add_help_section(
            "Environment variables",
            f"""
The following environment variables are used:
    * PYLINTHOME
    Path to the directory where persistent data for the run will be stored. If
not found, it defaults to '{DEFAULT_PYLINT_HOME}' or '{OLD_DEFAULT_PYLINT_HOME}'
(in the current working directory).
    * PYLINTRC
    Path to the configuration file. See the documentation for the method used
to search for configuration file.
""",
            level=1,
        )
        linter.add_help_section(
            "Output",
            "Using the default text output, the message format is :                          \n"
            "                                                                                \n"
            "        MESSAGE_TYPE: LINE_NUM:[OBJECT:] MESSAGE                                \n"
            "                                                                                \n"
            "There are 5 kind of message types :                                             \n"
            "    * (C) convention, for programming standard violation                        \n"
            "    * (R) refactor, for bad code smell                                          \n"
            "    * (W) warning, for python specific problems                                 \n"
            "    * (E) error, for probable bugs in the code                                  \n"
            "    * (F) fatal, if an error occurred which prevented pylint from doing further\n"
            "processing.\n",
            level=1,
        )
        linter.add_help_section(
            "Output status code",
            "Pylint should leave with following status code:                                 \n"
            "    * 0 if everything went fine                                                 \n"
            "    * 1 if a fatal message was issued                                           \n"
            "    * 2 if an error message was issued                                          \n"
            "    * 4 if a warning message was issued                                         \n"
            "    * 8 if a refactor message was issued                                        \n"
            "    * 16 if a convention message was issued                                     \n"
            "    * 32 on usage error                                                         \n"
            "                                                                                \n"
            "status 1 to 16 will be bit-ORed so you can know which different categories has\n"
            "been issued by analysing pylint output status code\n",
            level=1,
        )
        # read configuration
        linter.disable("I")
        linter.enable("c-extension-no-member")
        try:
            linter.read_config_file(verbose=self.verbose)
        except OSError as ex:
            print(ex, file=sys.stderr)
            sys.exit(32)

        config_parser = linter.cfgfile_parser
        # run init hook, if present, before loading plugins
        if config_parser.has_option("MASTER", "init-hook"):
            cb_init_hook(
                "init-hook", utils._unquote(config_parser.get("MASTER", "init-hook"))
            )
        # is there some additional plugins in the file configuration, in
        if config_parser.has_option("MASTER", "load-plugins"):
            plugins = utils._splitstrip(config_parser.get("MASTER", "load-plugins"))
            linter.load_plugin_modules(plugins)
        # now we can load file config and command line, plugins (which can
        # provide options) have been registered
        linter.load_config_file()

        if reporter:
            # if a custom reporter is provided as argument, it may be overridden
            # by file parameters, so re-set it here, but before command line
            # parsing so it's still overrideable by command line option
            linter.set_reporter(reporter)
        try:
            args = linter.load_command_line_configuration(args)
        except SystemExit as exc:
            if exc.code == 2:  # bad options
                exc.code = 32
            raise
        if not args:
            print(linter.help())
            sys.exit(32)

        if linter.config.jobs < 0:
            print(
                f"Jobs number ({linter.config.jobs}) should be greater than or equal to 0",
                file=sys.stderr,
            )
            sys.exit(32)
        if linter.config.jobs > 1 or linter.config.jobs == 0:
            if multiprocessing is None:
                print(
                    "Multiprocessing library is missing, fallback to single process",
                    file=sys.stderr,
                )
                linter.set_option("jobs", 1)
            elif linter.config.jobs == 0:
                linter.config.jobs = _cpu_count()

        # We have loaded configuration from config file and command line. Now, we can
        # load plugin specific configuration.
        linter.load_plugin_configuration()

        # Now that plugins are loaded, get list of all fail_on messages, and enable them
        linter.enable_fail_on_messages()

        if self._output:
            try:
                with open(self._output, "w", encoding="utf-8") as output:
                    linter.reporter.out = output
                    linter.check(args)
                    score_value = linter.generate_reports()
            except OSError as ex:
                print(ex, file=sys.stderr)
                sys.exit(32)
        else:
            linter.check(args)
            score_value = linter.generate_reports()

        if do_exit is not UNUSED_PARAM_SENTINEL:
            warnings.warn(
                "do_exit is deprecated and it is going to be removed in a future version.",
                DeprecationWarning,
            )
            exit = do_exit

        if exit:
            if linter.config.exit_zero:
                sys.exit(0)
            elif linter.any_fail_on_issues():
                # We need to make sure we return a failing exit code in this case.
                # So we use self.linter.msg_status if that is non-zero, otherwise we just return 1.
                sys.exit(self.linter.msg_status or 1)
            elif score_value is not None:
                if score_value >= linter.config.fail_under:
                    sys.exit(0)
                else:
                    # We need to make sure we return a failing exit code in this case.
                    # So we use self.linter.msg_status if that is non-zero, otherwise we just return 1.
                    sys.exit(self.linter.msg_status or 1)
            else:
                sys.exit(self.linter.msg_status)

    def version_asked(self, _, __):
        """callback for version (i.e. before option parsing)"""
        self._version_asked = True

    def cb_set_rcfile(self, name, value):
        """callback for option preprocessing (i.e. before option parsing)"""
        self._rcfile = value

    def cb_set_output(self, name, value):
        """callback for option preprocessing (i.e. before option parsing)"""
        self._output = value

    def cb_add_plugins(self, name, value):
        """callback for option preprocessing (i.e. before option parsing)"""
        self._plugins.extend(utils._splitstrip(value))

    def cb_error_mode(self, *args, **kwargs):
        """error mode:
        * disable all but error messages
        * disable the 'miscellaneous' checker which can be safely deactivated in
          debug
        * disable reports
        * do not save execution information
        """
        self.linter.error_mode()

    def cb_generate_config(self, *args, **kwargs):
        """optik callback for sample config file generation"""
        self.linter.generate_config(skipsections=("COMMANDS",))
        sys.exit(0)

    def cb_generate_manpage(self, *args, **kwargs):
        """optik callback for sample config file generation"""
        self.linter.generate_manpage(__pkginfo__)
        sys.exit(0)

    def cb_help_message(self, option, optname, value, parser):
        """optik callback for printing some help about a particular message"""
        self.linter.msgs_store.help_message(utils._splitstrip(value))
        sys.exit(0)

    def cb_full_documentation(self, option, optname, value, parser):
        """optik callback for printing full documentation"""
        print_full_documentation(self.linter)
        sys.exit(0)

    def cb_list_messages(self, option, optname, value, parser):
        """optik callback for printing available messages"""
        self.linter.msgs_store.list_messages()
        sys.exit(0)

    def cb_list_messages_enabled(self, option, optname, value, parser):
        """optik callback for printing available messages"""
        self.linter.list_messages_enabled()
        sys.exit(0)

    def cb_list_groups(self, *args, **kwargs):
        """List all the check groups that pylint knows about

        These should be useful to know what check groups someone can disable
        or enable.
        """
        for check in self.linter.get_checker_names():
            print(check)
        sys.exit(0)

    def cb_verbose_mode(self, *args, **kwargs):
        self.verbose = True

    def cb_enable_all_extensions(self, option_name: str, value: None) -> None:
        """Callback to load and enable all available extensions"""
        for filename in os.listdir(os.path.dirname(extensions.__file__)):
            # pylint: disable=fixme
            # TODO: Remove the check for deprecated check_docs after the extension has been removed
            if (
                filename.endswith(".py")
                and not filename.startswith("_")
                and not filename.startswith("check_docs")
            ):
                extension_name = f"pylint.extensions.{filename[:-3]}"
                if extension_name not in self._plugins:
                    self._plugins.append(extension_name)
