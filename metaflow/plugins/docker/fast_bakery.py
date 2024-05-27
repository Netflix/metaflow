import hashlib
import json
import requests

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    DOCKER_IMAGE_BAKERY_TYPE,
    DOCKER_IMAGE_BAKERY_URL,
    DOCKER_IMAGE_BAKERY_AUTH,
    get_pinned_conda_libs,
)

BAKERY_METAFILE = ".imagebakery-cache"


class FastBakeryException(MetaflowException):
    headline = "Docker Image Bakery ran into an exception"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(FastBakeryException, self).__init__(msg)


def read_metafile():
    try:
        with open(BAKERY_METAFILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def cache_image_tag(spec_hash, image, request):
    current_meta = read_metafile()
    current_meta[spec_hash] = {
        "kind": DOCKER_IMAGE_BAKERY_TYPE,
        "image": image,
        "bakery_request": request,
    }

    with open(BAKERY_METAFILE, "w") as f:
        json.dump(current_meta, f)


def get_cache_image_tag(spec_hash):
    current_meta = read_metafile()

    return current_meta.get(spec_hash, {}).get("image", None)


def generate_spec_hash(
    base_image=None, python_version=None, packages={}, resolver_type=None
):
    sorted_keys = sorted(packages.keys())
    base_str = "".join(
        [DOCKER_IMAGE_BAKERY_TYPE, python_version, base_image or "", resolver_type]
    )
    sortspec = base_str.join("%s%s" % (k, packages[k]) for k in sorted_keys).encode(
        "utf-8"
    )
    hash = hashlib.md5(sortspec).hexdigest()

    return hash


def bake_image(
    python=None,
    packages={},
    datastore_type=None,
    base_image=None,
    resolver_type="conda",
):
    if DOCKER_IMAGE_BAKERY_URL is None:
        raise FastBakeryException("Image bakery URL is not set.")
    # Gather base deps
    deps = {}
    if datastore_type is not None:
        deps = get_pinned_conda_libs(python, datastore_type)
    deps.update(packages)

    # Try getting image tag from cache
    spec_hash = generate_spec_hash(base_image, python, deps, resolver_type)
    image = get_cache_image_tag(spec_hash)
    if image:
        return image

    def _format(pkg, ver):
        if any(ver.startswith(c) for c in [">", "<", "~", "@", "="]):
            return "%s%s" % (pkg, ver)
        return "%s==%s" % (pkg, ver)

    package_matchspecs = [_format(pkg, ver) for pkg, ver in deps.items()]

    data = {
        "imageKind": DOCKER_IMAGE_BAKERY_TYPE,
        "pythonVersion": python,
    }
    if resolver_type == "conda":
        data.update({"condaMatchspecs": package_matchspecs})
    elif resolver_type == "pypi":
        data.update({"pipRequirements": package_matchspecs})
    else:
        raise FastBakeryException("Unknown resolver type: %s" % resolver_type)

    if base_image is not None:
        data.update({"baseImage": {"imageReference": base_image}})

    invoker = BAKERY_INVOKERS.get(DOCKER_IMAGE_BAKERY_AUTH)
    if not invoker:
        raise FastBakeryException(
            "Selected Bakery Authentication method is not supported: %s",
            DOCKER_IMAGE_BAKERY_AUTH,
        )
    image = invoker(data)
    # Cache tag
    cache_image_tag(spec_hash, image, data)

    return image


def default_invoker(payload):
    headers = {"Content-Type": "application/json"}
    response = requests.post(DOCKER_IMAGE_BAKERY_URL, json=payload, headers=headers)

    return _handle_bakery_response(response)


def aws_iam_invoker(payload):
    # AWS_IAM requires a signed request to be made
    # ref: https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
    headers = {"Content-Type": "application/json"}
    payload = json.dumps(payload)

    import boto3
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    # credits to https://github.com/boto/botocore/issues/1784#issuecomment-659132830,
    # We need to jump through some hoops when calling the endpoint with IAM auth
    # as botocore does not offer a direct utility for signing arbitrary requests
    req = AWSRequest("POST", DOCKER_IMAGE_BAKERY_URL, headers, payload)
    SigV4Auth(
        credentials, service_name="lambda", region_name=session.region_name
    ).add_auth(req)

    response = requests.post(DOCKER_IMAGE_BAKERY_URL, data=payload, headers=req.headers)

    return _handle_bakery_response(response)


def _handle_bakery_response(response):
    if response.status_code >= 500:
        raise FastBakeryException(response.text)
    body = response.json()
    if response.status_code >= 400:
        try:
            kind = body["kind"]
            msg = body["message"]
            raise FastBakeryException("*%s*\n%s" % (kind, msg))
        except KeyError:
            # error body is not formatted by the imagebakery
            raise FastBakeryException(body)
    image = body["containerImage"]

    return image


BAKERY_INVOKERS = {"AWS_IAM": aws_iam_invoker, None: default_invoker}
