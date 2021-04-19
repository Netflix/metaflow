import sys

try:
    from StringIO import StringIO
except:
    from io import StringIO

from .exception import MetaflowException

class PyLintWarn(MetaflowException):
    headline="Pylint is not happy"

class PyLint(object):

    def __init__(self, fname):
        self._fname = fname
        try:
            from pylint.lint import Run
            self._run = Run
        except:
            self._run = None

    def has_pylint(self):
        return self._run is not None

    def run(self, logger=None, warnings=False, pylint_config=[]):
        args = [self._fname]
        if not warnings:
            args.append('--errors-only')
        if pylint_config:
            args.extend(pylint_config)
        stdout = sys.stdout
        stderr = sys.stderr
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            pylint_is_happy = True
            pylint_exception_msg = ""
            run = self._run(args, None, False)
        except Exception as e:
            pylint_is_happy = False
            pylint_exception_msg = repr(e)
        output = sys.stdout.getvalue()
        sys.stdout = stdout
        sys.stderr = stderr

        warnings = False
        for line in self._filter_lines(output):
            logger(line, indent=True)
            warnings = True

        if warnings:
            raise PyLintWarn('*Fix Pylint warnings listed above or say --no-pylint.*')

        return pylint_is_happy, pylint_exception_msg

    def _filter_lines(self, output):
        for line in output.splitlines():
            # Ignore headers
            if '***' in line:
                continue
            # Ignore complaints about decorators missing in the metaflow module.
            # Automatic generation of decorators confuses Pylint.
            if "(no-name-in-module)" in line:
                continue
            # Ignore complaints related to dynamic and JSON-types parameters
            if "Instance of 'Parameter' has no" in line:
                continue
            # Ditto for IncludeFile
            if "Instance of 'IncludeFile' has no" in line:
                continue
            # Ditto for dynamically added properties in 'current'
            if "Instance of 'Current' has no" in line:
                continue
            # Ignore complaints of self.next not callable
            if "self.next is not callable" in line:
                continue
            yield line
