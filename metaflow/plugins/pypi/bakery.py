import os
import requests

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DOCKER_IMAGE_BAKERY_URL


class BakeryException(MetaflowException):
    headline = "Docker Image Bakery ran into an exception"

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(BakeryException, self).__init__(msg)


class Bakery(object):
    # Mostly a no-op class to support prebaked images for conda environments.
    def __init__(self):
        pass

    def solve(self, id_, packages, python, platform):
        # Solve the environment
        return {}

    def download(self, id_, packages, python, platform):
        # nothing to download due to image being built remotely.
        return

    def create(self, id_, packages, python, platform):
        # create environment
        # raise Exception("not ready to execute yet, still testing.")
        pass

    def info(self):
        return "no info"

    def path_to_environment(self, id_, platform=None):
        return "/conda-prefix"

    def metadata(self, id_, packages, python, platform):
        # environment metadata
        return {}

    def interpreter(self, id_):
        return os.path.join(self.path_to_environment(id_), "bin/python")

    def platform(self):
        return self.info()["platform"]


def bake_image(packages={}):
    if DOCKER_IMAGE_BAKERY_URL is None:
        raise BakeryException("Image bakery URL is not set.")

    package_matchspecs = ["%s%s" % (pkg, ver) for pkg, ver in packages.items()]

    headers = {"Content-Type": "application/json"}
    data = {"conda_matchspecs": ["python"] + package_matchspecs}
    response = requests.post(DOCKER_IMAGE_BAKERY_URL, json=data, headers=headers)

    body = response.json()
    if response.status_code > 400:
        raise BakeryException(body)
    image = body["container_image"]

    return image
