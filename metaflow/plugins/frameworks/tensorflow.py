from metaflow.plugins.parallel_decorator import ParallelDecorator
import os
import json
import socket
import time
import random


class TensorflowParallelDecorator(ParallelDecorator):
    name = "tensorflow_parallel"
    defaults = {}
    IS_PARALLEL = True

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        return super().task_decorate(
            step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
        )

    def setup_distributed_env(self, flow):
        setup_tf_distributed(flow)


def setup_tf_distributed(run):
    from metaflow import current, S3

    # TODO: spin up a simple server to exchange the ips and ports.
    print(
        "Setup TF distributed, using S3 to rendezvous. Number of nodes: %d"
        % current.parallel.num_nodes
    )
    num_nodes = current.parallel.num_nodes
    node_index = current.parallel.node_index
    local_ip = socket.gethostbyname_ex(socket.gethostname())[-1][0]
    s3 = S3(run=run)

    # Create a port id based on run id and node index to avoid clashes if the
    # workers run on same machine
    my_port = 40000 + (int(current.run_id) % 100) * 100 + node_index
    info_dict = {"node": node_index, "address": "{}:{}".format(local_ip, my_port)}
    key = os.path.join("tf_nodes", "node_{}.json".format(node_index))
    print("Storing:", info_dict, " to:", key)
    s3.put(key, json.dumps(info_dict))

    # Then poll for others
    all_workers = {node_index: info_dict}
    while len(all_workers) < num_nodes:
        print(
            "Got information for {} workers out of {}".format(
                len(all_workers), num_nodes
            )
        )

        for other_node in range(num_nodes):
            if other_node not in all_workers:
                node_key = os.path.join("tf_nodes", "node_{}.json".format(other_node))
                node_info = s3.get(node_key, return_missing=True)  # use get_many
                if node_info.exists:
                    print("Found information for node {}".format(other_node))
                    all_workers[other_node] = json.loads(node_info.blob)
                    print(all_workers)
                print("Could not load {}, trying soon again...".format(node_key))

        time.sleep(4.0 + random.random() * 3.0)

    my_task = {"type": "worker", "index": node_index}
    cluster = {
        "worker": [all_workers[node_id]["address"] for node_id in range(num_nodes)]
    }
    json_config = json.dumps({"cluster": cluster, "task": my_task})
    os.environ["TF_CONFIG"] = json_config
    print(json_config)
