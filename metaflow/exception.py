import sys
import traceback
import types

# worker processes that exit with this exit code are not retried
METAFLOW_EXIT_DISALLOW_RETRY = 202

# worker processes that exit with this code should be retried (if retry counts left)
METAFLOW_EXIT_ALLOW_RETRY = 203

class MetaflowExceptionWrapper(Exception):
    def __init__(self, exc=None):
        if exc is not None:
            self.exception = str(exc)
            self.type = '%s.%s' % (exc.__class__.__module__,
                                   exc.__class__.__name__)
            if sys.exc_info()[0] is None:
                self.stacktrace = None
            else:
                self.stacktrace = traceback.format_exc()

    # Base Exception defines its own __reduce__ and __setstate__
    # which don't work nicely with derived exceptions. We override
    # the magic methods related to pickle to get desired behavior.
    def __reduce__(self):
        return MetaflowExceptionWrapper, (None,), self.__dict__

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state

    def __str__(self):
        if self.stacktrace:
            return self.stacktrace
        else:
            return '[no stacktrace]\n%s: %s' % (self.type, self.exception)

class MetaflowException(Exception):
    headline = 'Flow failed'
    def __init__(self, msg='', lineno=None):
        self.message = msg
        self.line_no = lineno
        super(MetaflowException, self).__init__()

    def __str__(self):
        prefix = 'line %d: ' % self.line_no if self.line_no else ''
        return '%s%s' % (prefix, self.message)

class ParameterFieldFailed(MetaflowException):
    headline = "Parameter field failed"

    def __init__(self, name, field):
        exc = traceback.format_exc()
        msg = "When evaluating the field *%s* for the Parameter *%s*, "\
              "the following exception occurred:\n\n%s" % (field, name, exc)
        super(ParameterFieldFailed, self).__init__(msg)

class ParameterFieldTypeMismatch(MetaflowException):
    headline = "Parameter field with a mismatching type"

    def __init__(self, msg):
        super(ParameterFieldTypeMismatch, self).__init__(msg)

class ExternalCommandFailed(MetaflowException):
    headline = "External command failed"

    def __init__(self, msg):
        super(ExternalCommandFailed, self).__init__(msg)

class MetaflowNotFound(MetaflowException):
    headline = 'Object not found'

class MetaflowNamespaceMismatch(MetaflowException):
    headline = 'Object not in the current namespace'

    def __init__(self, namespace):
        msg = "Object not in namespace '%s'" % namespace
        super(MetaflowNamespaceMismatch, self).__init__(msg)

class MetaflowInternalError(MetaflowException):
    headline = 'Internal error'

class MetaflowUnknownUser(MetaflowException):
    headline = 'Unknown user'

    def __init__(self):
        msg = "Metaflow could not determine your user name based on "\
              "environment variables ($USERNAME etc.)"
        super(MetaflowUnknownUser, self).__init__(msg)

class InvalidDecoratorAttribute(MetaflowException):
    headline = "Unknown decorator attribute"
    def __init__(self, deconame, attr, defaults):
        msg = "Decorator '{deco}' does not support the attribute '{attr}'. "\
              "These attributes are supported: {defaults}."\
              .format(deco=deconame,
                      attr=attr,
                      defaults=', '.join(defaults))
        super(InvalidDecoratorAttribute, self).__init__(msg)

class CommandException(MetaflowException):
    headline = "Invalid command"

class MetaflowDataMissing(MetaflowException):
    headline = "Data missing"

class UnhandledInMergeArtifactsException(MetaflowException):
    headline = "Unhandled artifacts in merge"

    def __init__(self, msg, unhandled):
        super(UnhandledInMergeArtifactsException, self).__init__(msg)
        self.artifact_names = unhandled

class MissingInMergeArtifactsException(MetaflowException):
    headline = "Missing artifacts in merge"

    def __init__(self, msg, unhandled):
        super(MissingInMergeArtifactsException, self).__init__(msg)
        self.artifact_names = unhandled

# Import any exceptions defined by a Metaflow custom package
try:
    import metaflow_custom.exceptions as extension_module
except ImportError as e:
    ver = sys.version_info[0] * 10 + sys.version_info[1]
    if ver >= 36:
        # e.path is not None if the error stems from some other place than here
        # so don't error ONLY IF the error is importing this module (but do
        # error if there is a transitive import error)
        if not (isinstance(e, ModuleNotFoundError) and e.path is None):
            print(
                "Cannot load metaflow_custom exceptions -- "
                "if you want to ignore, uninstall metaflow_custom package")
            raise
else:
    # We load into globals whatever we have in extension_module
    # We specifically exclude any modules that may be included (like sys, os, etc)
    # *except* for ones that are part of metaflow_custom (basically providing
    # an aliasing mechanism)
    lazy_load_custom_modules = {}
    for n, o in extension_module.__dict__.items():
        if not n.startswith('__') and not isinstance(o, types.ModuleType):
            globals()[n] = o
        elif isinstance(o, types.ModuleType) and o.__package__ and \
                o.__package__.startswith('metaflow_custom'):
            lazy_load_custom_modules['metaflow.%s' % n] = o
    if lazy_load_custom_modules:
        from metaflow import _LazyLoader
        sys.meta_path.append(_LazyLoader(lazy_load_custom_modules))
finally:
    # Erase all temporary names to avoid leaking things
    for _n in ['ver', 'n', 'o', 'e', 'lazy_load_custom_modules',
               'extension_module', '_LazyLoader']:
        try:
            del globals()[_n]
        except KeyError:
            pass
    del globals()['_n']
