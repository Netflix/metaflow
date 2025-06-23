import inspect
import importlib
import sys
import os


def main(flow_file, fn_name):
    flow_import = flow_file.rstrip(".py")

    tempflow = importlib.import_module(flow_import)

    # hook_fn
    hook_fn = getattr(tempflow, fn_name)

    argspec = inspect.getfullargspec(hook_fn)

    # Check if fn expects a run object as an arg.
    if "run" in argspec.args or argspec.varkw is not None:
        from metaflow import Run, namespace

        namespace(None)
        run_pathspec = os.getenv("RUN_PATHSPEC")
        if run_pathspec is None:
            raise Exception("RUN_PATHSPEC is not set in env.")
        try:
            _run = Run(run_pathspec)
        except Exception as ex:
            print(ex)
            _run = None

        hook_fn(run=_run)
    else:
        hook_fn()


if __name__ == "__main__":
    try:
        flow_file, fn_name = sys.argv[1:3]
    except Exception:
        print("Usage: exit_hook_script.py <flow_file> <function_name>")
        sys.exit(1)

    main(flow_file, fn_name)
