import os
import inspect
import importlib
import sys


def main(flow_file, fn_name_or_path, run_pathspec):
    hook_fn = None

    try:
        module_path, function_name = fn_name_or_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        hook_fn = getattr(module, function_name)
    except (ImportError, AttributeError, ValueError):
        try:
            module_name = os.path.splitext(os.path.basename(flow_file))[0]
            spec = importlib.util.spec_from_file_location(module_name, flow_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            hook_fn = getattr(module, fn_name_or_path)
        except (AttributeError, IOError) as e:
            print(
                f"[exit_hook] Could not load function '{fn_name_or_path}' "
                f"as an import path or from '{flow_file}': {e}"
            )
            sys.exit(1)

    argspec = inspect.getfullargspec(hook_fn)

    # Check if fn expects a run object as an arg.
    if "run" in argspec.args or argspec.varkw is not None:
        from metaflow import Run

        try:
            _run = Run(run_pathspec, _namespace_check=False)
        except Exception as ex:
            print(ex)
            _run = None

        hook_fn(run=_run)
    else:
        hook_fn()


if __name__ == "__main__":
    try:
        flow_file, fn_name, run_pathspec = sys.argv[1:4]
    except Exception:
        print("Usage: exit_hook_script.py <flow_file> <function_name> <run_pathspec>")
        sys.exit(1)

    main(flow_file, fn_name, run_pathspec)
