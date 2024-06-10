import json
import requests


class FastBakeryException(Exception):
    headline = "Fast Bakery ran into an exception"


class FastBakery:
    def __init__(self, url, auth_type=None):
        self.url = url
        BAKERY_INVOKERS = {"AWS_IAM": self.aws_iam_invoker, None: self.default_invoker}
        if auth_type not in BAKERY_INVOKERS:
            raise FastBakeryException(
                "Selected Bakery Authentication method is not supported: %s",
                auth_type,
            )
        self.invoker = BAKERY_INVOKERS[auth_type]

    def bake(
        self,
        python=None,
        packages={},
        base_image=None,
        resolver_type="conda",
        image_kind="oci-zstd",
    ):
        def _format(pkg, ver):
            if any(ver.startswith(c) for c in [">", "<", "~", "@", "="]):
                return "%s%s" % (pkg, ver)
            return "%s==%s" % (pkg, ver)

        package_matchspecs = [_format(pkg, ver) for pkg, ver in packages.items()]

        data = {
            "imageKind": image_kind,
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

        image = self.invoker(data)

        return image, data

    def default_invoker(self, payload):
        headers = {"Content-Type": "application/json"}
        response = requests.post(self.url, json=payload, headers=headers)

        return _handle_bakery_response(response)

    def aws_iam_invoker(self, payload):
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
        req = AWSRequest("POST", self.url, headers, payload)
        SigV4Auth(
            credentials, service_name="lambda", region_name=session.region_name
        ).add_auth(req)

        response = requests.post(self.url, data=payload, headers=req.headers)

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
