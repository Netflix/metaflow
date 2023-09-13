from metaflow.decorators import StepDecorator
from metaflow.metaflow_environment import InvalidEnvironmentException


class PyPIStepDecorator(StepDecorator):
    name = "pypi"
    defaults = {"packages": {}, "indices": []}  # wheels

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
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

        # The --environment= requirement ensures that valid virtual environments are
        # created for every step to execute it, greatly simplifying the @pypi
        # implementation.
        if environment.TYPE not in _supported_virtual_envs:
            raise InvalidEnvironmentException(
                "@%s decorator requires %s"
                % (
                    self.name,
                    "or ".join(
                        ["--environment=%s" % env for env in _supported_virtual_envs]
                    ),
                )
            )

        # The init_environment hook for Environment creates the relevant virtual
        # environments. The step_init hook sets up the relevant state for that hook to
        # do it's magic.
