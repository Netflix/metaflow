import os
import shutil
import sys
import tempfile
import zlib
import base64
import re

from functools import wraps
from io import BytesIO
from itertools import takewhile
from typing import Dict, Any, Tuple, Optional, List, Generator


try:
    # python2
    unicode_type = unicode
    bytes_type = str
    from urllib import quote, unquote

    # unquote_bytes should be a function that takes a urlencoded byte
    # string, encoded in UTF-8, url-decodes it and returns it as a
    # unicode object. Confusingly, how to accomplish this differs
    # between Python2 and Python3.
    #
    # Test with this input URL:
    # b'crazypath/%01%C3%B'
    # it should produce
    # u'crazypath/\x01\xff'
    def unquote_bytes(x):
        return to_unicode(unquote(to_bytes(x)))

    # this is used e.g. by mflog/save_logs.py to identify paths
    class Path(object):
        def __init__(self, path):
            self.path = path

        def __str__(self):
            return self.path

    from pipes import quote as _quote
except NameError:
    # python3
    unicode_type = str
    bytes_type = bytes
    from urllib.parse import quote, unquote
    from pathlib import Path

    def unquote_bytes(x):
        return unquote(to_unicode(x))

    from shlex import quote as _quote


class TempDir(object):
    # Provide a temporary directory since Python 2.7 does not have it inbuilt
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)


def cached_property(getter):
    @wraps(getter)
    def exec_once(self):
        saved_name = "__%s" % getter.__name__
        if not hasattr(self, saved_name):
            setattr(self, saved_name, getter(self))
        return getattr(self, saved_name)

    return property(exec_once)


def all_equal(it):
    """
    Return True if all elements of the given iterator are equal.
    """
    it = iter(it)
    try:
        first = next(it)
    except StopIteration:
        return True
    for x in it:
        if x != first:
            return False
    return True


def url_quote(url):
    """
    Encode a unicode URL to a safe byte string
    """
    # quote() works reliably only with (byte)strings in Python2,
    # hence we need to .encode('utf-8') first. To see by yourself,
    # try quote(u'\xff') in python2. Python3 converts the output
    # always to Unicode, hence we need the outer to_bytes() too.
    #
    # We mark colon as a safe character to keep simple ASCII urls
    # nice looking, e.g. "http://google.com"
    return to_bytes(quote(to_bytes(url), safe="/:"))


def url_unquote(url_bytes):
    """
    Decode a byte string encoded with url_quote to a unicode URL
    """
    return unquote_bytes(url_bytes)


def is_stringish(x):
    """
    Returns true if the object is a unicode or a bytes object
    """
    return isinstance(x, bytes_type) or isinstance(x, unicode_type)


def to_fileobj(x):
    """
    Convert any string-line object to a byte-returning fileobj
    """
    return BytesIO(to_bytes(x))


def to_unicode(x):
    """
    Convert any object to a unicode object
    """
    if isinstance(x, bytes_type):
        return x.decode("utf-8")
    else:
        return unicode_type(x)


def to_bytes(x):
    """
    Convert any object to a byte string
    """
    if isinstance(x, unicode_type):
        return x.encode("utf-8")
    elif isinstance(x, bytes_type):
        return x
    elif isinstance(x, float):
        return repr(x).encode("utf-8")
    else:
        return str(x).encode("utf-8")


def get_username():
    """
    Return the name of the current user, or None if the current user
    could not be determined.
    """
    from metaflow.metaflow_config import USER

    # note: the order of the list matters
    ENVVARS = ["SUDO_USER", "USERNAME", "USER"]
    for user in [USER] + [os.environ.get(x) for x in ENVVARS]:
        if user and user != "root":
            return user
    return None


def resolve_identity_as_tuple():
    from metaflow.exception import MetaflowUnknownUser

    prod_token = os.environ.get("METAFLOW_PRODUCTION_TOKEN")
    if prod_token:
        return "production", prod_token
    user = get_username()
    if user and user != "root":
        return "user", user
    else:
        raise MetaflowUnknownUser()


def resolve_identity():
    identity_type, identity_value = resolve_identity_as_tuple()
    return "%s:%s" % (identity_type, identity_value)


def parse_spin_pathspec(pathspec: str, flow_name: str) -> Tuple:
    """
    Parse various pathspec formats for the spin command.

    Parameters
    ----------
    pathspec : str
        The pathspec string in one of the following formats:
        - step_name (e.g., 'start')
        - run_id/step_name (e.g., '221165/start')
        - run_id/step_name/task_id (e.g., '221165/start/1350987')
        - flow_name/run_id/step_name (e.g., 'ScalableFlow/221165/start')
        - flow_name/run_id/step_name/task_id (e.g., 'ScalableFlow/221165/start/1350987')
    flow_name : str
        The name of the current flow.

    Returns
    -------
    Tuple
        A tuple of (step_name, full_pathspec_or_none)

    Raises
    ------
    CommandException
        If the pathspec format is invalid or flow name doesn't match.
    """
    from .exception import CommandException

    parts = pathspec.split("/")

    if len(parts) == 1:
        # Just step name: 'start'
        step_name = parts[0]
        parsed_pathspec = None
    elif len(parts) == 2:
        # run_id/step_name: '221165/start'
        run_id, step_name = parts
        parsed_pathspec = f"{flow_name}/{run_id}/{step_name}"
    elif len(parts) == 3:
        # Could be run_id/step_name/task_id or flow_name/run_id/step_name
        if parts[0] == flow_name:
            # flow_name/run_id/step_name
            _, run_id, step_name = parts
            parsed_pathspec = f"{flow_name}/{run_id}/{step_name}"
        else:
            # run_id/step_name/task_id
            run_id, step_name, task_id = parts
            parsed_pathspec = f"{flow_name}/{run_id}/{step_name}/{task_id}"
    elif len(parts) == 4:
        # flow_name/run_id/step_name/task_id
        parsed_flow_name, run_id, step_name, task_id = parts
        if parsed_flow_name != flow_name:
            raise CommandException(
                f"Flow name '{parsed_flow_name}' in pathspec does not match current flow '{flow_name}'."
            )
        parsed_pathspec = pathspec
    else:
        raise CommandException(
            f"Invalid pathspec format: '{pathspec}'. \n"
            "Expected formats:\n"
            "  - step_name (e.g., 'start')\n"
            "  - run_id/step_name (e.g., '221165/start')\n"
            "  - run_id/step_name/task_id (e.g., '221165/start/1350987')\n"
            "  - flow_name/run_id/step_name (e.g., 'ScalableFlow/221165/start')\n"
            "  - flow_name/run_id/step_name/task_id (e.g., 'ScalableFlow/221165/start/1350987')"
        )

    return step_name, parsed_pathspec


def get_latest_task_pathspec(
    flow_name: str, step_name: str, run_id: str = None
) -> "metaflow.Task":
    """
    Returns a task pathspec from the latest run (or specified run) of the flow for the queried step.
    If the queried step has several tasks, the task pathspec of the first task is returned.

    Parameters
    ----------
    flow_name : str
        The name of the flow.
    step_name : str
        The name of the step.
    run_id : str, optional
        The run ID to use. If None, uses the latest run.

    Returns
    -------
    Task
        A Metaflow Task instance containing the latest task for the queried step.

    Raises
    ------
    MetaflowNotFound
        If no task or run is found for the queried step.
    """
    from metaflow import Flow, Step
    from metaflow.exception import MetaflowNotFound

    if not run_id:
        flow = Flow(flow_name)
        run = flow.latest_run
        if run is None:
            raise MetaflowNotFound(f"No run found for flow {flow_name}")
        run_id = run.id

    try:
        task = Step(f"{flow_name}/{run_id}/{step_name}").task
        return task
    except:
        raise MetaflowNotFound(f"No task found for step {step_name} in run {run_id}")


def get_latest_run_id(echo, flow_name):
    from metaflow.plugins.datastores.local_storage import LocalStorage

    local_root = LocalStorage.datastore_root
    if local_root is None:
        local_root = LocalStorage.get_datastore_root_from_config(
            echo, create_on_absent=False
        )
    if local_root:
        path = os.path.join(local_root, flow_name, "latest_run")
        if os.path.exists(path):
            with open(path) as f:
                return f.read()
    return None


def write_latest_run_id(obj, run_id):
    from metaflow.plugins.datastores.local_storage import LocalStorage

    if LocalStorage.datastore_root is None:
        LocalStorage.datastore_root = LocalStorage.get_datastore_root_from_config(
            obj.echo
        )
    path = LocalStorage.path_join(LocalStorage.datastore_root, obj.flow.name)
    try:
        os.makedirs(path)
    except OSError as x:
        if x.errno != 17:
            # Directories exists in other case which is fine
            raise
    with open(os.path.join(path, "latest_run"), "w") as f:
        f.write(str(run_id))


def get_object_package_version(obj):
    """
    Return the top level package name and package version that defines the
    class of the given object.
    """
    try:
        module_name = obj.__class__.__module__

        if "." in module_name:
            top_package_name = module_name.split(".")[0]
        else:
            top_package_name = module_name

    except AttributeError:
        return None, None

    try:
        top_package_version = sys.modules[top_package_name].__version__
        return top_package_name, top_package_version

    except AttributeError:
        return top_package_name, None


def compress_list(lst, separator=",", rangedelim=":", zlibmarker="!", zlibmin=500):
    from metaflow.exception import MetaflowInternalError

    bad_items = [x for x in lst if separator in x or rangedelim in x or zlibmarker in x]
    if bad_items:
        raise MetaflowInternalError(
            "Item '%s' includes a delimiter character "
            "so it can't be compressed" % bad_items[0]
        )
    # Three output modes:
    lcp = longest_common_prefix(lst)
    if len(lst) < 2 or not lcp:
        # 1. Just a comma-separated list
        res = separator.join(lst)
    else:
        # 2. Prefix and a comma-separated list of suffixes
        lcplen = len(lcp)
        residuals = [e[lcplen:] for e in lst]
        res = rangedelim.join((lcp, separator.join(residuals)))
    if len(res) < zlibmin:
        return res
    else:
        # 3. zlib-compressed, base64-encoded, prefix-encoded list

        # interestingly, a typical zlib-encoded list of suffixes
        # has plenty of redundancy. Decoding the data *twice* helps a
        # lot
        compressed = zlib.compress(zlib.compress(to_bytes(res)))
        return zlibmarker + base64.b64encode(compressed).decode("utf-8")


def decompress_list(lststr, separator=",", rangedelim=":", zlibmarker="!"):
    # Three input modes:
    if lststr[0] == zlibmarker:
        # 3. zlib-compressed, base64-encoded
        lstbytes = base64.b64decode(lststr[1:])
        decoded = zlib.decompress(zlib.decompress(lstbytes)).decode("utf-8")
    else:
        decoded = lststr

    if rangedelim in decoded:
        prefix, suffixes = decoded.split(rangedelim)
        # 2. Prefix and a comma-separated list of suffixes
        return [prefix + suffix for suffix in suffixes.split(separator)]
    else:
        # 1. Just a comma-separated list
        return decoded.split(separator)


def longest_common_prefix(lst):
    if lst:
        return "".join(
            a for a, _ in takewhile(lambda t: t[0] == t[1], zip(min(lst), max(lst)))
        )
    else:
        return ""


def get_metaflow_root():
    return os.path.dirname(os.path.dirname(__file__))


def dict_to_cli_options(params):
    # Prevent circular imports
    from .user_configs.config_options import ConfigInput

    for k, v in params.items():
        # Omit boolean options set to false or None, but preserve options with an empty
        # string argument.
        if v is not False and v is not None:
            # we need special handling for 'with' since it is a reserved
            # keyword in Python, so we call it 'decospecs' in click args
            if k == "decospecs":
                k = "with"
            if k in ("config", "config_value"):
                # Special handling here since we gather them all in one option but actually
                # need to send them one at a time using --config-value <name> kv.<name>
                # Note it can be either config or config_value depending
                # on click processing order.
                for config_name in v.keys():
                    yield "--config-value"
                    yield to_unicode(config_name)
                    yield to_unicode(ConfigInput.make_key_name(config_name))
                continue
            if k == "local_config_file":
                # Skip this value -- it should only be used locally and never when
                # forming another command line
                continue
            k = k.replace("_", "-")
            v = v if isinstance(v, (list, tuple, set)) else [v]
            for value in v:
                yield "--%s" % k
                if not isinstance(value, bool):
                    value = to_unicode(value)

                    # Of the value starts with $, assume the caller wants shell variable
                    # expansion to happen, so we pass it as is.
                    # NOTE: We strip '\' to allow for various storages to use escaped
                    # shell variables as well.
                    if value.lstrip("\\").startswith("$"):
                        yield value
                    else:
                        # Otherwise, assume it is a literal value and quote it safely
                        yield _quote(value)


# This function is imported from https://github.com/cookiecutter/whichcraft
def which(cmd, mode=os.F_OK | os.X_OK, path=None):
    """Given a command, mode, and a PATH string, return the path which
    conforms to the given mode on the PATH, or None if there is no such
    file.
    `mode` defaults to os.F_OK | os.X_OK. `path` defaults to the result
    of os.environ.get("PATH"), or can be overridden with a custom search
    path.
    Note: This function was backported from the Python 3 source code.
    """
    # Check that a given file can be accessed with the correct mode.
    # Additionally check that `file` is not a directory, as on Windows
    # directories pass the os.access check.
    try:  # Forced testing
        from shutil import which as w

        return w(cmd, mode, path)
    except ImportError:

        def _access_check(fn, mode):
            return os.path.exists(fn) and os.access(fn, mode) and not os.path.isdir(fn)

        # If we're given a path with a directory part, look it up directly
        # rather than referring to PATH directories. This includes checking
        # relative to the current directory, e.g. ./script
        if os.path.dirname(cmd):
            if _access_check(cmd, mode):
                return cmd
            return None

        if path is None:
            path = os.environ.get("PATH", os.defpath)
        if not path:
            return None

        path = path.split(os.pathsep)

        files = [cmd]
        seen = set()
        for dir in path:
            normdir = os.path.normcase(dir)
            if normdir not in seen:
                seen.add(normdir)
                for thefile in files:
                    name = os.path.join(dir, thefile)
                    if _access_check(name, mode):
                        return name

        return None


def to_camelcase(obj):
    """
    Convert all keys of a json to camel case from snake case.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        res = obj.__class__()
        for k in obj:
            res[re.sub(r"(?!^)_([a-zA-Z])", lambda x: x.group(1).upper(), k)] = (
                to_camelcase(obj[k])
            )
    elif isinstance(obj, (list, set, tuple)):
        res = obj.__class__(to_camelcase(v) for v in obj)
    else:
        return obj
    return res


def to_pascalcase(obj):
    """
    Convert all keys of a json to pascal case.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        res = obj.__class__()
        for k in obj:
            res[re.sub("([a-zA-Z])", lambda x: x.groups()[0].upper(), k, count=1)] = (
                to_pascalcase(obj[k])
            )
    elif isinstance(obj, (list, set, tuple)):
        res = obj.__class__(to_pascalcase(v) for v in obj)
    else:
        return obj
    return res


def tar_safe_extract(tar, path=".", members=None, *, numeric_owner=False):
    def is_within_directory(abs_directory, target):
        prefix = os.path.commonprefix([abs_directory, os.path.abspath(target)])
        return prefix == abs_directory

    abs_directory = os.path.abspath(path)
    if any(
        not is_within_directory(abs_directory, os.path.join(path, member.name))
        for member in tar.getmembers()
    ):
        raise Exception("Attempted path traversal in TAR file")

    tar.extractall(path, members, numeric_owner=numeric_owner)


def to_pod(value):
    """
    Convert a python object to plain-old-data (POD) format.

    Parameters
    ----------
    value : Any
        Value to convert to POD format. The value can be a string, number, list,
        dictionary, or a nested structure of these types.
    """
    # Prevent circular imports
    from metaflow.parameters import DeployTimeField

    if isinstance(value, (str, int, float)):
        return value
    if isinstance(value, dict):
        return {to_pod(k): to_pod(v) for k, v in value.items()}
    if isinstance(value, (list, set, tuple)):
        return [to_pod(v) for v in value]
    if isinstance(value, DeployTimeField):
        return value.print_representation
    return str(value)


from metaflow._vendor.packaging.version import parse as version_parse


def read_artifacts_module(file_path: str) -> Dict[str, Any]:
    """
    Read a Python module from the given file path and return its ARTIFACTS variable.

    Parameters
    ----------
    file_path : str
        The path to the Python file containing the ARTIFACTS variable.

    Returns
    -------
    Dict[str, Any]
        A dictionary containing the ARTIFACTS variable from the module.

    Raises
    -------
    MetaflowInternalError
        If the file cannot be read or does not contain the ARTIFACTS variable.
    """
    import importlib.util
    import os

    try:
        module_name = os.path.splitext(os.path.basename(file_path))[0]
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        variables = vars(module)
        if "ARTIFACTS" not in variables:
            raise MetaflowInternalError(
                f"Module {file_path} does not contain ARTIFACTS variable"
            )
        return variables.get("ARTIFACTS")
    except Exception as e:
        raise MetaflowInternalError(f"Error reading file {file_path}") from e


# this is os.walk(follow_symlinks=True) with cycle detection
def walk_without_cycles(
    top_root: str,
    exclude_dirs: Optional[List[str]] = None,
) -> Generator[Tuple[str, List[str], List[str]], None, None]:
    seen = set()

    default_skip_dirs = ["__pycache__"]

    def _recurse(root, skip_dirs):
        for parent, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for d in dirs:
                path = os.path.join(parent, d)
                if os.path.islink(path):
                    # Breaking loops: never follow the same symlink twice
                    #
                    # NOTE: this also means that links to sibling links are
                    # not followed. In this case:
                    #
                    #   x -> y
                    #   y -> oo
                    #   oo/real_file
                    #
                    # real_file is only included twice, not three times
                    reallink = os.path.realpath(path)
                    if reallink not in seen:
                        seen.add(reallink)
                        for x in _recurse(path, default_skip_dirs):
                            yield x
            yield parent, dirs, files

    skip_dirs = set(default_skip_dirs + (exclude_dirs or []))
    for x in _recurse(top_root, skip_dirs):
        skip_dirs = default_skip_dirs
        yield x
