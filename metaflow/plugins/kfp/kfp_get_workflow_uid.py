def get_workflow_uid(
    work_flow_name: str,
    s3_sensor_path: str,
) -> str:
    """
    The environment variables that this depends on:
        POD_NAMESPACE
    """
    import os
    import subprocess

    command = [
        "kubectl",
        "get",
        "workflow",
        work_flow_name,
        "--output",
        "jsonpath='{.metadata.uid}'",
    ]

    namespace = os.environ.get("POD_NAMESPACE", default=None)
    if namespace:
        command.extend(["--namespace", str(namespace)])

    print("command=", " ".join(command))

    result = subprocess.run(command, stdout=subprocess.PIPE)
    uid = result.stdout.decode("utf-8").strip("'")
    print("uid=", uid)
    return uid
