from metaflow.decorators import FlowDecorator, StepDecorator
from metaflow.metaflow_environment import InvalidEnvironmentException


class PyPIStepDecorator(StepDecorator):
    """
    Specifies the PyPI packages for the step.

    Information in this decorator will augment any
    attributes set in the `@pyi_base` flow-level decorator. Hence,
    you can use `@pypi_base` to set packages required by all
    steps and use `@pypi` to specify step-specific overrides.

    Parameters
    ----------
    packages : Dict[str, str], default: {}
        Packages to use for this step. The key is the name of the package
        and the value is the version to use.
    python : str, optional, default: None
        Version of Python to use, e.g. '3.7.4'. A default value of None implies
        that the version used will correspond to the version of the Python interpreter used to start the run.
    """

    name = "pypi"
    defaults = {"packages": {}, "python": None, "disabled": None}  # wheels

    def __init__(self, attributes=None, statically_defined=False):
        self._user_defined_attributes = (
            attributes.copy() if attributes is not None else {}
        )
        super().__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # The init_environment hook for Environment creates the relevant virtual
        # environments. The step_init hook sets up the relevant state for that hook to
        # do it's magic.

        self.flow = flow
        self.step = step

        # Support flow-level decorator
        if "pypi_base" in self.flow._flow_decorators:
            pypi_base = self.flow._flow_decorators["pypi_base"][0]
            super_attributes = pypi_base.attributes
            self._user_defined_attributes = {
                **self._user_defined_attributes,
                **pypi_base._user_defined_attributes,
            }
            self.attributes["packages"] = {
                **super_attributes["packages"],
                **self.attributes["packages"],
            }
            self.attributes["python"] = (
                self.attributes["python"] or super_attributes["python"]
            )
            self.attributes["disabled"] = (
                self.attributes["disabled"]
                if self.attributes["disabled"] is not None
                else super_attributes["disabled"]
            )

        # Set default for `disabled` argument.
        if not self.attributes["disabled"]:
            self.attributes["disabled"] = False

        # At the moment, @pypi uses a conda environment as a virtual environment. This
        # is to ensure that we can have a dedicated Python interpreter within the
        # virtual environment. The conda environment is currently created through
        # micromamba. As a follow up, we can look into creating a virtualenv using
        # venv.

        # Currently, @pypi relies on pip for package resolution. We can introduce
        # support for Poetry in the near future, if desired. Poetry is great for
        # interactive use cases, but not so much for programmatic use cases like the
        # one here. We can consider introducing a UX where @pypi is able to consume
        # poetry.lock files in the future.

        _supported_virtual_envs = ["conda"]  # , "venv"]

        # To placate people who don't want to see a shred of conda in UX, we symlink
        # --environment=pypi to --environment=conda
        _supported_virtual_envs.extend(["pypi"])

        # TODO: Hardcoded for now to support the fast bakery environment.
        # We should introduce a more robust mechanism for appending supported environments, for example from within extensions.
        _supported_virtual_envs.extend(["fast-bakery"])

        # The --environment= requirement ensures that valid virtual environments are
        # created for every step to execute it, greatly simplifying the @pypi
        # implementation.
        if environment.TYPE not in _supported_virtual_envs:
            raise InvalidEnvironmentException(
                "@%s decorator requires %s"
                % (
                    self.name,
                    " or ".join(
                        ["--environment=%s" % env for env in _supported_virtual_envs]
                    ),
                )
            )
        # TODO: This code snippet can be done away with by altering the constructor of
        #       MetaflowEnvironment. A good first-task exercise.
        # Avoid circular import
        from metaflow.plugins.datastores.local_storage import LocalStorage

        environment.set_local_root(LocalStorage.get_datastore_root_from_config(logger))

    def is_attribute_user_defined(self, name):
        return name in self._user_defined_attributes


class PyPIFlowDecorator(FlowDecorator):
    """
    Specifies the PyPI packages for all steps of the flow.

    Use `@pypi_base` to set common packages required by all
    steps and use `@pypi` to specify step-specific overrides.
    Parameters
    ----------
    packages : Dict[str, str], default: {}
        Packages to use for this flow. The key is the name of the package
        and the value is the version to use.
    python : str, optional, default: None
        Version of Python to use, e.g. '3.7.4'. A default value of None implies
        that the version used will correspond to the version of the Python interpreter used to start the run.
    """

    name = "pypi_base"
    defaults = {"packages": {}, "python": None, "disabled": None}

    def __init__(self, attributes=None, statically_defined=False):
        self._user_defined_attributes = (
            attributes.copy() if attributes is not None else {}
        )
        super().__init__(attributes, statically_defined)

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        from metaflow import decorators

        decorators._attach_decorators(flow, ["pypi"])

        # @pypi uses a conda environment to create a virtual environment.
        # The conda environment can be created through micromamba.
        _supported_virtual_envs = ["conda"]

        # To placate people who don't want to see a shred of conda in UX, we symlink
        # --environment=pypi to --environment=conda
        _supported_virtual_envs.extend(["pypi"])

        # TODO: Hardcoded for now to support the fast bakery environment.
        # We should introduce a more robust mechanism for appending supported environments, for example from within extensions.
        _supported_virtual_envs.extend(["fast-bakery"])

        # The --environment= requirement ensures that valid virtual environments are
        # created for every step to execute it, greatly simplifying the @conda
        # implementation.
        if environment.TYPE not in _supported_virtual_envs:
            raise InvalidEnvironmentException(
                "@%s decorator requires %s"
                % (
                    self.name,
                    " or ".join(
                        ["--environment=%s" % env for env in _supported_virtual_envs]
                    ),
                )
            )
