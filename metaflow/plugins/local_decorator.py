from metaflow.decorators import StepDecorator


class LocalDecorator(StepDecorator):
    """
    Specifies that this step should always be executed locally.

    Use this decorator to exclude a step from being run remotely even
    when the flow is invoked with a remote execution flag such as
    `--with batch` or `--with kubernetes`.

    Note that this decorator currently specifically blocks `@batch` and
    `@kubernetes` remote execution backends.

    Parameters
    ----------
    None
    """

    name = "local"
    defaults = {}
