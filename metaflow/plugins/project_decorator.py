from metaflow.exception import MetaflowException
from metaflow.decorators import FlowDecorator
from metaflow import current
from metaflow.util import get_username

import os
import re

class NamespaceValidationException(Exception):

    def __init__(self, attr_name):
        super().__init__()
        self.attribute_name = attr_name

class EmptyNamespaceAttributeException(NamespaceValidationException):
    
    def __init__(self, attr_name):
        super().__init__(attr_name)

class NamespaceAttributeTooLongException(NamespaceValidationException):

    def __init__(self, attr_name):
        super().__init__(attr_name)

class InvalidNamespaceAttributeValueException(NamespaceValidationException):

    def __init__(self, attr_name):
        super().__init__(attr_name)

# be careful when changing these limits. Other systems that see
# these names may rely on these limits
VALID_NAME_RE = "[^a-z0-9_]"
VALID_NAME_LEN = 128


class ProjectDecorator(FlowDecorator):
    """
    Specifies what flows belong to the same project.

    A project-specific namespace is created for all flows that
    use the same `@project(name)`.

    Parameters
    ----------
    name : str
        Project name. Make sure that the name is unique amongst all
        projects that use the same production scheduler. The name may
        contain only lowercase alphanumeric characters and underscores.
    """

    name = "project"
    defaults = {"name": None}

    options = {
        "production": dict(
            is_flag=True,
            default=False,
            show_default=True,
            help="Use the @project's production branch. To "
            "use a custom branch, use --branch.",
        ),
        "branch": dict(
            default=None,
            show_default=False,
            help="Use the given branch name under @project. "
            "The default is the user name if --production is "
            "not specified.",
        ),
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self._option_values = options
        project_name = self.attributes.get("name")
        project_flow_name, branch_name = format_name(
            flow.name,
            project_name,
            options["production"],
            options["branch"],
            get_username(),
        )
        is_user_branch = options["branch"] is None and not options["production"]
        echo(
            "Project: *%s*, Branch: *%s*" % (project_name, branch_name),
            fg="magenta",
            highlight="green",
        )
        current._update_env(
            {
                "project_name": project_name,
                "branch_name": branch_name,
                "is_user_branch": is_user_branch,
                "is_production": options["production"],
                "project_flow_name": project_flow_name,
            }
        )
        metadata.add_sticky_tags(
            sys_tags=["project:%s" % project_name, "project_branch:%s" % branch_name]
        )

    def get_top_level_options(self):
        return list(self._option_values.items())


def format_name(flow_name, project_name, deploy_prod, given_branch, user_name):
    if given_branch:
        if deploy_prod:
            branch = "prod.%s" % given_branch
        else:
            branch = "test.%s" % given_branch
    elif deploy_prod:
        branch = "prod"
    else:
        # For AWS Step Functions, we set the branch to the value of
        # environment variable `METAFLOW_OWNER`, since AWS Step Functions
        # has no notion of user name.
        branch = "user.%s" % os.environ.get("METAFLOW_OWNER", user_name)
    try:
        return apply_project_namespacing(flow_name, project_name, branch), branch
    except EmptyNamespaceAttributeException as e:
        if e.attribute_name == "project_name":
            # an empty string is not a valid project name
            raise MetaflowException(
                "@project needs a name. " "Try @project(name='some_name')"
            )
    except NamespaceAttributeTooLongException as e:
        if e.attribute_name == "project_name":
            raise MetaflowException(
                "The @project name must be shorter than " "%d characters." % VALID_NAME_LEN
            )
        elif e.attribute_name == "branch_name":
            raise MetaflowException(
                "Branch name is too long. "
                "The maximum is %d characters." % VALID_NAME_LEN
            )
    except InvalidNamespaceAttributeValueException as e:
        if e.attribute_name == "project_name":
            raise MetaflowException(
                "The @project name must contain only "
                "lowercase alphanumeric characters "
                "and underscores."
            )
        elif e.attribute_name == "branch_name":
            raise MetaflowException(
                "The branch name must contain only "
                "lowercase alphanumeric characters "
                "and underscores."
            )

def apply_project_namespacing(flow_name, project_name, branch):
    if not project_name:
        raise EmptyNamespaceAttributeException("project_name")
    elif re.search(VALID_NAME_RE, project_name):
        raise InvalidNamespaceAttributeValueException("project_name")
    elif len(project_name) > VALID_NAME_LEN:
        raise NamespaceAttributeTooLongException("project_name")
    if branch:
        prefix = ""
        # Strip off prefixes added by decorator before validating the branch name
        for p in ["prod.", "test.", "user."]:
            chunks = branch.split(p)
            if len(chunks) == 2:
                prefix = p
                branch = chunks[1]
                break
        if re.search(VALID_NAME_RE, branch):
            raise InvalidNamespaceAttributeValueException("branch_name")
        elif len(branch) > VALID_NAME_LEN:
            raise NamespaceAttributeTooLongException("branch_name")
        else:
            branch = "".join((prefix, branch))
    return ".".join((project_name, branch, flow_name)).lower()
