import json
from collections import defaultdict
from typing import List, Dict, Optional


class Container:
    def __init__(self, name: str, image: str):
        self.payload = defaultdict(lambda: defaultdict(dict))
        self.payload["name"] = name
        self.payload["image"] = image

    def command(self, command: List[str]) -> "Container":
        self.payload["command"] = command
        return self

    def args(self, args: List[str]) -> "Container":
        self.payload["args"] = args
        return self

    def env(self, env: Dict[str, str]) -> "Container":
        self.payload["env"] = env
        return self

    def readiness_probe(self, readiness_probe: "ReadinessProbe") -> "Container":
        self.payload["readiness_probe"] = (
            readiness_probe.to_dict() if readiness_probe else None
        )
        return self

    def volume_mounts(self, volume_mounts: List["VolumeMount"]) -> "Container":
        self.payload["volume_mounts"] = [vm.to_dict() for vm in volume_mounts]
        return self

    def resources(self, resources: "Resources") -> "Container":
        self.payload["resources"] = resources.to_dict() if resources else None
        return self

    def secrets(self, secrets: List["Secrets"]) -> "Container":
        self.payload["secrets"] = [secret.to_dict() for secret in secrets]
        return self

    def to_dict(self) -> Dict:
        result = {"name": self.payload["name"], "image": self.payload["image"]}
        if "command" in self.payload and self.payload["command"]:
            result["command"] = self.payload["command"]
        if "args" in self.payload and self.payload["args"]:
            result["args"] = self.payload["args"]
        if "env" in self.payload and self.payload["env"]:
            result["env"] = self.payload["env"]
        if "readiness_probe" in self.payload and self.payload["readiness_probe"]:
            result["readiness_probe"] = self.payload["readiness_probe"]
        if "volume_mounts" in self.payload and self.payload["volume_mounts"]:
            result["volume_mounts"] = self.payload["volume_mounts"]
        if "resources" in self.payload and self.payload["resources"]:
            result["resources"] = self.payload["resources"]
        if "secrets" in self.payload and self.payload["secrets"]:
            result["secrets"] = self.payload["secrets"]
        return result


class ReadinessProbe:
    def __init__(self, port: int, path: str):
        self.payload = defaultdict(dict)
        self.payload["port"] = port
        self.payload["path"] = path

    def to_dict(self) -> Dict:
        return dict(self.payload)


class VolumeMount:
    def __init__(self, name: str, mount_path: str):
        self.payload = defaultdict(dict)
        self.payload["name"] = name
        self.payload["mount_path"] = mount_path

    def to_dict(self) -> Dict:
        return dict(self.payload)


class Resources:
    def __init__(
        self,
        requests: Optional[Dict[str, str]] = None,
        limits: Optional[Dict[str, str]] = None,
    ):
        self.payload = defaultdict(dict)
        if requests:
            self.payload["requests"] = requests
        if limits:
            self.payload["limits"] = limits

    def to_dict(self) -> Dict:
        result = {}
        if "requests" in self.payload and self.payload["requests"]:
            result["requests"] = self.payload["requests"]
        if "limits" in self.payload and self.payload["limits"]:
            result["limits"] = self.payload["limits"]
        return result


class Secrets:
    def __init__(self, snowflake_secret: str):
        self.payload = {"snowflake_secret": snowflake_secret}

    def secret_key_ref(self, secret_key_ref: str) -> "Secrets":
        self.payload["secret_key_ref"] = secret_key_ref
        return self

    def env_var_name(self, env_var_name: str) -> "Secrets":
        self.payload["env_var_name"] = env_var_name
        return self

    def directory_path(self, directory_path: str) -> "Secrets":
        self.payload["directory_path"] = directory_path
        return self

    def to_dict(self) -> Dict:
        return {k: v for k, v in self.payload.items() if v}


class Endpoint:
    def __init__(self, name: str, port: int):
        self.payload = defaultdict(dict)
        self.payload["name"] = name
        self.payload["port"] = port

    def public(self, public: bool) -> "Endpoint":
        self.payload["public"] = public
        return self

    def protocol(self, protocol: str) -> "Endpoint":
        self.payload["protocol"] = protocol
        return self

    def to_dict(self) -> Dict:
        return dict(self.payload)


class Volume:
    def __init__(self, name: str, source: str):
        self.payload = defaultdict(dict)
        self.payload["name"] = name
        self.payload["source"] = source

    def size(self, size: str) -> "Volume":
        self.payload["size"] = size
        return self

    def block_config(self, block_config: Dict) -> "Volume":
        self.payload["block_config"] = block_config
        return self

    def uid(self, uid: int) -> "Volume":
        self.payload["uid"] = uid
        return self

    def gid(self, gid: int) -> "Volume":
        self.payload["gid"] = gid
        return self

    def to_dict(self) -> Dict:
        result = {"name": self.payload["name"], "source": self.payload["source"]}
        if "size" in self.payload:
            result["size"] = self.payload["size"]
        if "block_config" in self.payload:
            result["block_config"] = self.payload["block_config"]
        if "uid" in self.payload:
            result["uid"] = self.payload["uid"]
        if "gid" in self.payload:
            result["gid"] = self.payload["gid"]
        return result


class LogExporters:
    def __init__(self):
        self.payload = {}

    def event_table_config(self, log_level: str) -> "LogExporters":
        self.payload["eventTableConfig"] = {"logLevel": log_level}
        return self

    def to_dict(self) -> Dict:
        return dict(self.payload)


class ServiceRole:
    def __init__(self, name: str):
        self.payload = {"name": name}

    def endpoints(self, endpoints: List[str]) -> "ServiceRole":
        self.payload["endpoints"] = endpoints
        return self

    def to_dict(self) -> Dict:
        return dict(self.payload)


class SnowparkServiceSpec:
    def __init__(self):
        self.payload = defaultdict(lambda: defaultdict(list))

    def containers(self, containers: List[Container]) -> "SnowparkServiceSpec":
        self.payload["containers"] = [container.to_dict() for container in containers]
        return self

    def endpoints(self, endpoints: List[Endpoint]) -> "SnowparkServiceSpec":
        self.payload["endpoints"] = [endpoint.to_dict() for endpoint in endpoints]
        return self

    def volumes(self, volumes: List[Volume]) -> "SnowparkServiceSpec":
        self.payload["volumes"] = [volume.to_dict() for volume in volumes]
        return self

    def log_exporters(self, log_exporters: LogExporters) -> "SnowparkServiceSpec":
        self.payload["logExporters"] = log_exporters.to_dict()
        return self

    def service_roles(self, service_roles: List[ServiceRole]) -> "SnowparkServiceSpec":
        self.payload["serviceRoles"] = [role.to_dict() for role in service_roles]
        return self

    def to_dict(self) -> Dict:
        return {"spec": dict(self.payload)}


def generate_spec_file(spec: SnowparkServiceSpec, filename: str, format: str = "yaml"):
    import yaml

    spec_dict = spec.to_dict()
    with open(filename, "w") as file:
        if format == "json":
            json.dump(spec_dict, file, indent=2)
        elif format == "yaml":
            yaml.dump(spec_dict, file, default_flow_style=False)


if __name__ == "__main__":
    # Example usage
    container = (
        Container(name="example-container", image="example-image")
        .command(["python3", "app.py"])
        .env({"ENV_VARIABLE": "value"})
        .readiness_probe(ReadinessProbe(port=8080, path="/health"))
        .resources(
            Resources(requests={"memory": "2G", "cpu": "1"}, limits={"memory": "4G"})
        )
    )

    endpoint = Endpoint(name="example-endpoint", port=8080).public(True)
    volume = Volume(name="example-volume", source="local")
    spec = (
        SnowparkServiceSpec()
        .containers([container])
        .endpoints([endpoint])
        .volumes([volume])
    )

    generate_spec_file(spec, "service_spec.yaml", format="yaml")
