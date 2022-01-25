# Licensed under the LGPL: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
# For details: https://github.com/PyCQA/astroid/blob/main/LICENSE

"""Astroid hooks for understanding boto3.ServiceRequest()"""
from metaflow._vendor.astroid import extract_node
from metaflow._vendor.astroid.manager import AstroidManager
from metaflow._vendor.astroid.nodes.scoped_nodes import ClassDef

BOTO_SERVICE_FACTORY_QUALIFIED_NAME = "boto3.resources.base.ServiceResource"


def service_request_transform(node):
    """Transform ServiceResource to look like dynamic classes"""
    code = """
    def __getattr__(self, attr):
        return 0
    """
    func_getattr = extract_node(code)
    node.locals["__getattr__"] = [func_getattr]
    return node


def _looks_like_boto3_service_request(node):
    return node.qname() == BOTO_SERVICE_FACTORY_QUALIFIED_NAME


AstroidManager().register_transform(
    ClassDef, service_request_transform, _looks_like_boto3_service_request
)
