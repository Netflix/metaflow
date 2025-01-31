from metaflow.exception import MetaflowException
from metaflow.decorators import FlowDecorator
from metaflow import current
from metaflow.util import get_username

import os
import re

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

    branch : Optional[str], default None
        The branch to use. If not specified, the branch is set to
        `user.<username>` unless `production` is set to `True`. This can
        also be set on the command line using `--branch` as a top-level option.
        It is an error to specify `branch` in the decorator and on the command line.

    production : bool, default False
        Whether or not the branch is the production branch. This can also be set on the
        command line using `--production` as a top-level option. It is an error to specify
        `production` in the decorator and on the command line.
        The project branch name will be:
          - if `branch` is specified:
            - if `production` is True: `prod.<branch>`
            - if `production` is False: `test.<branch>`
          - if `branch` is not specified:
            - if `production` is True: `prod`
            - if `production` is False: `user.<username>`

    MF Add To Current
    -----------------
    project_name -> str
        The name of the project assigned to this flow, i.e. `X` in `@project(name=X)`.

        @@ Returns
        -------
        str
            Project name.

    project_flow_name -> str
        The flow name prefixed with the current project and branch. This name identifies
        the deployment on a production scheduler.

        @@ Returns
        -------
        str
            Flow name prefixed with project information.

    branch_name -> str
        The current branch, i.e. `X` in `--branch=X` set during deployment or run.

        @@ Returns
        -------
        str
            Branch name.

    is_user_branch -> bool
        True if the flow is deployed without a specific `--branch` or a `--production`
        flag.

        @@ Returns
        -------
        bool
            True if the deployment does not correspond to a specific branch.

    is_production -> bool
        True if the flow is deployed with the `--production` flag

        @@ Returns
        -------
        bool
            True if the flow is deployed with `--production`.
    """

    name = "project"

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

    defaults = {"name": None, **{k: v["default"] for k, v in options.items()}}

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self._option_values = options
        project_name = self.attributes.get("name")
        for op in options:
            if (
                op in self._user_defined_attributes
                and options[op] != self.defaults[op]
                and self.attributes[op] != options[op]
            ):
                # Exception if:
                #  - the user provides a value in the attributes field
                #  - AND the user provided a value in the command line (non default)
                #  - AND the values are different
                # Note that this won't raise an error if the user provided the default
                # value in the command line and provided one in attribute but although
                # slightly inconsistent, it is not incorrect.
                raise MetaflowException(
                    "You cannot pass %s as both a command-line argument and an attribute "
                    "of the @project decorator." % op
                )
        if "branch" in self._user_defined_attributes:
            project_branch = self.attributes["branch"]
        else:
            project_branch = options["branch"]

        if "production" in self._user_defined_attributes:
            project_production = self.attributes["production"]
        else:
            project_production = options["production"]

        project_flow_name, branch_name = format_name(
            flow.name,
            project_name,
            project_production,
            project_branch,
            get_username(),
        )
        is_user_branch = project_branch is None and not project_production
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
                "is_production": project_production,
                "project_flow_name": project_flow_name,
            }
        )
        metadata.add_sticky_tags(
            sys_tags=["project:%s" % project_name, "project_branch:%s" % branch_name]
        )

    def get_top_level_options(self):
        return list(self._option_values.items())


def format_name(flow_name, project_name, deploy_prod, given_branch, user_name):
    if not project_name:
        # an empty string is not a valid project name
        raise MetaflowException(
            "@project needs a name. " "Try @project(name='some_name')"
        )
    elif re.search(VALID_NAME_RE, project_name):
        raise MetaflowException(
            "The @project name must contain only "
            "lowercase alphanumeric characters "
            "and underscores."
        )
    elif len(project_name) > VALID_NAME_LEN:
        raise MetaflowException(
            "The @project name must be shorter than " "%d characters." % VALID_NAME_LEN
        )

    if given_branch:
        if re.search(VALID_NAME_RE, given_branch):
            raise MetaflowException(
                "The branch name must contain only "
                "lowercase alphanumeric characters "
                "and underscores."
            )
        elif len(given_branch) > VALID_NAME_LEN:
            raise MetaflowException(
                "Branch name is too long. "
                "The maximum is %d characters." % VALID_NAME_LEN
            )
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

    return ".".join((project_name, branch, flow_name)), branch
