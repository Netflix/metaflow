import sys


def generate_input_paths(run_id, step_name, task_id_entropy, num_parallel):
    # => run_id/step/:foo,bar
    control_id = "control-{}-0".format(task_id_entropy)
    worker_ids = [
        "worker-{}-{}".format(task_id_entropy, i) for i in range(int(num_parallel) - 1)
    ]
    ids = [control_id] + worker_ids
    return "{}/{}/:{}".format(run_id, step_name, ",".join(ids))


if __name__ == "__main__":
    print(generate_input_paths(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]))
