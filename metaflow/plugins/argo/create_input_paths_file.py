import sys
from metaflow import Run
from metaflow.util import compress_list
from tempfile import NamedTemporaryFile

# This utility uses the Metadata Service to fetch completed tasks for the provided step_names for a specific run
# and writes them out to a file, returning the file path as a result.
# This is required due to Foreach split output steps not being deterministic anymore after introducing conditional branching to Metaflow, as during graph parsing we now only know the set of possible steps that leads to the executing step.


def fetch_input_paths(step_names, run_pathspec):
    steps = step_names.split(",")
    run = Run(run_pathspec, _namespace_check=False)

    input_paths = []
    for step in steps:
        try:
            # for input paths we require the pathspec without the Flow name
            input_paths.extend(f"{run.id}/{step}/{task.id}" for task in run[step])
        except KeyError:
            # a step might not have ever executed due to it being conditional.
            pass

    return input_paths


if __name__ == "__main__":
    input_paths = fetch_input_paths(sys.argv[1], sys.argv[2])
    # we use the Metaflow internal compress_list due to --input-paths-filename processing relying on decompress_list.
    compressed = compress_list(input_paths)

    with NamedTemporaryFile(delete=False) as f:
        f.write(compressed.encode("utf-8"))
        print(f.name)
