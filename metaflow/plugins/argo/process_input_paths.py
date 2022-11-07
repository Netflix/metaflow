import sys
import re


def process_input_paths(input_paths):
    # Convert Argo Workflows provided input-paths string to something that Metaflow
    # understands
    #
    # flow/step/[{task-id:foo},{task-id:bar}] => flow/step/:foo,bar

    flow, run_id, task_ids = input_paths.split("/")
    task_ids = re.sub("[\[\]{}]", "", task_ids)
    task_ids = task_ids.split(",")
    tasks = [t.split(":")[1] for t in task_ids]
    return "{}/{}/:{}".format(flow, run_id, ",".join(tasks))


if __name__ == "__main__":
    print(process_input_paths(sys.argv[1]))
