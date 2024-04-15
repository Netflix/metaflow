import hashlib
import json
import requests

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    DOCKER_IMAGE_BAKERY_TYPE,
    DOCKER_IMAGE_BAKERY_URL,
    get_pinned_conda_libs,
)

BAKERY_METAFILE = ".imagebakery-cache"


class BakeryException(MetaflowException):
    headline = "Docker Image Bakery ran into an exception"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(BakeryException, self).__init__(msg)


def read_metafile():
    try:
        with open(BAKERY_METAFILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def cache_image_tag(spec_hash, image, packages):
    current_meta = read_metafile()
    current_meta[spec_hash] = {
        "kind": DOCKER_IMAGE_BAKERY_TYPE,
        "packages": packages,
        "image": image,
    }

    with open(BAKERY_METAFILE, "w") as f:
        json.dump(current_meta, f)


def get_cache_image_tag(spec_hash):
    current_meta = read_metafile()

    return current_meta.get(spec_hash, {}).get("image", None)


def generate_spec_hash(packages={}):
    sorted_keys = sorted(packages.keys())
    sortspec = DOCKER_IMAGE_BAKERY_TYPE.join(
        f"{k}{packages[k]}" for k in sorted_keys
    ).encode("utf-8")
    hash = hashlib.md5(sortspec).hexdigest()

    return hash


def bake_image(python=None, packages={}, datastore_type=None):
    if DOCKER_IMAGE_BAKERY_URL is None:
        raise BakeryException("Image bakery URL is not set.")
    # Gather base deps
    deps = {}
    if datastore_type is not None:
        deps = get_pinned_conda_libs(python, datastore_type)
    deps.update(packages)
    if python is not None:
        deps.update({"python": python})

    # TODO: Cache image tags locally and add cache revoke functionality
    # Try getting image tag from cache
    spec_hash = generate_spec_hash(packages)
    image = get_cache_image_tag(spec_hash)
    if image:
        return image

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

    # Cache tag
    cache_image_tag(spec_hash, image, packages)

    return image
