import sys
from hashlib import md5


def generate_input_paths(step_name, timestamp, input_paths, max_splits):
    # => run_id/step/:foo,bar
    run_id = input_paths.split("/")[0]
    foreach_base_id = "{}-{}-{}".format(step_name, timestamp, input_paths)

    ids = [_generate_task_id(foreach_base_id, i) for i in range(int(max_splits))]
    return "{}/{}/:{}".format(run_id, step_name, ",".join(ids))


def _generate_task_id(base, idx):
    # For foreach splits generate the expected input-paths based on max_split and base_id via.
    # newline required at the end due to 'echo' appending one in the shell side task_id creation.
    task_str = "%s-%s\n" % (base, idx)
    hash = md5(task_str.encode("utf-8")).hexdigest()[-8:]
    return "t-" + hash


if __name__ == "__main__":
    print(generate_input_paths(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]))
