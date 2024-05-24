from metaflow.decorators import StepDecorator


class LocalDecorator(StepDecorator):
    """
    Specifies that the step will run locally.

    The decorator will force the step to run locally even if --with batch or
    --with kubernetes options are specified.
    """

    name = "local"
