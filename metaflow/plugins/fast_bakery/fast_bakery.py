from typing import Dict, Optional
import requests


class FastBakeryException(Exception):
    pass


class FastBakery:
    def __init__(self, url: str):
        self.url = url
        self.headers = {"Content-Type": "application/json", "Connection": "keep-alive"}
        self._reset_payload()

    def _reset_payload(self):
        self._payload = {}

    def python_version(self, version: str):
        self._payload["pythonVersion"] = version
        return self

    def pypi_packages(self, packages: Dict[str, str]):
        self._payload.setdefault("pipRequirements", []).extend(
            self._format_packages(packages)
        )
        return self

    def conda_packages(self, packages: Dict[str, str]):
        self._payload.setdefault("condaMatchspecs", []).extend(
            self._format_packages(packages)
        )
        return self

    def base_image(self, image: str):
        self._payload["baseImage"] = {"imageReference": image}
        return self

    def image_kind(self, kind: str):
        self._payload["imageKind"] = kind
        return self

    def ignore_cache(self):
        self._payload["cacheBehavior"] = {
            "responseMaxAgeSeconds": 0,
            "layerMaxAgeSeconds": 0,
            "baseImageMaxAgeSeconds": 0,
        }
        return self

    @staticmethod
    def _format_packages(packages: Dict[str, str]) -> list:
        if not packages:
            return []

        def format_package(pkg: str, ver: str) -> str:
            return (
                f"{pkg}{ver}"
                if any(ver.startswith(c) for c in [">", "<", "~", "@", "="])
                else f"{pkg}=={ver}"
            )

        return [format_package(pkg, ver) for pkg, ver in packages.items()]

    def bake(self) -> str:
        if "imageKind" not in self._payload:
            self._payload["imageKind"] = "oci-zstd"  # Set default if not specified

        image = self._make_request(self._payload)
        self._reset_payload()
        return image

    def _make_request(self, payload: Dict) -> str:
        try:
            from metaflow.metaflow_config import SERVICE_HEADERS

            headers = {**self.headers, **(SERVICE_HEADERS or {})}
        except ImportError:
            headers = self.headers
        response = requests.post(self.url, json=payload, headers=headers)
        self._handle_error_response(response)

        return response.json()

    @staticmethod
    def _handle_error_response(response: requests.Response):
        if response.status_code >= 500:
            raise FastBakeryException(f"Server error: {response.text}")

        body = response.json()
        status_code = body.get("error", {}).get("statusCode", response.status_code)
        if status_code >= 400:
            try:
                raise FastBakeryException(
                    f"*{body['error']['details']['kind']}*\n{body['error']['details']['message']}"
                )
            except KeyError:
                raise FastBakeryException(f"Unexpected error: {body}")
