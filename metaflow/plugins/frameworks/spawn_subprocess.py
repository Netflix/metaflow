import click
import pickle
import sys
import importlib

"""
Script to spawn a function call in a subprocess. Similar to multiprocessing module,
but avoids re-executing the original process. Inputs and outputs are pickled into a file.
"""


@click.command()
@click.argument("target_module_directory")
@click.argument("target_module")
@click.argument("target_function")
@click.argument("pickled_argument_file")
def spawn(
    target_module_directory, target_module, target_function, pickled_argument_file
):
    """
    Used ot spawn a python function in a separate process, passing arguments
    as pickled objects. Return values are passed as pickled object in
    the file pickled_argument_file + '.out'
    """
    try:
        # Load target module
        sys.path.append(target_module_directory)
        module = importlib.import_module(target_module)
        fun = getattr(module, target_function)

        # Read input arguments
        with open(pickled_argument_file, "rb") as args_f:
            kwargs = pickle.load(args_f)
        results = fun(**kwargs)
        if results:
            with open(pickled_argument_file + ".out", "wb") as output_f:
                pickle.dump(results, file=output_f)
    except Exception as ex:
        print(ex)
        raise


if __name__ == "__main__":
    spawn()
