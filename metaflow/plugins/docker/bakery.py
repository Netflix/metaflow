import requests

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    DOCKER_IMAGE_BAKERY_TYPE,
    DOCKER_IMAGE_BAKERY_URL,
    get_pinned_conda_libs,
)


class BakeryException(MetaflowException):
    headline = "Docker Image Bakery ran into an exception"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(BakeryException, self).__init__(msg)


def bake_image(python=None, packages={}, datastore_type=None):
    # TODO: Cache image tags locally and add cache revoke functionality
    if DOCKER_IMAGE_BAKERY_URL is None:
        raise BakeryException("Image bakery URL is not set.")
    # Gather base deps
    deps = {}
    if datastore_type is not None:
        deps = get_pinned_conda_libs(python, datastore_type)
    deps.update(packages)
    if python is not None:
        deps.update({"python": python})

    def _format(pkg, ver):
        if any(ver.startswith(c) for c in [">", "<", "~", "@", "="]):
            return "%s%s" % (pkg, ver)
        return "%s==%s" % (pkg, ver)

    package_matchspecs = [_format(pkg, ver) for pkg, ver in deps.items()]

    headers = {"Content-Type": "application/json"}
    data = {
        "condaMatchspecs": package_matchspecs,
        "imageKind": DOCKER_IMAGE_BAKERY_TYPE,
    }
    # TODO: introduce auth
    response = requests.post(DOCKER_IMAGE_BAKERY_URL, json=data, headers=headers)

    body = response.json()
    if response.status_code >= 400:
        kind = body["kind"]
        msg = body["message"]
        raise BakeryException("*%s*\n%s" % (kind, msg))
    image = body["containerImage"]

    return image
