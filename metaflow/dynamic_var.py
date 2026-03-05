class DynamicVar:
    """Marker for decorator attributes resolved from parent step artifacts at runtime."""

    def __init__(self, var_name):
        self.var_name = var_name

    def __repr__(self):
        return "dynamic_var('%s')" % self.var_name


def dynamic_var(var_name):
    """
    Mark a decorator parameter to be resolved dynamically from a parent step artifact.

    Use this in decorator parameters to indicate that the value should come from
    ``self.<var_name>`` set in the parent step. For example::

        @batch(cpu=dynamic_var("num_cpu"))
        @step
        def compute(self):
            ...

    means "use the value of ``self.num_cpu`` from the parent step as the cpu parameter."

    Parameters
    ----------
    var_name : str
        Name of the flow artifact to read from the parent step.

    Returns
    -------
    DynamicVar
        A marker object that will be resolved at runtime.
    """
    return DynamicVar(var_name)


def get_dynamic_vars(attributes):
    """Returns {attr_key: DynamicVar} for all dynamic var attributes."""
    return {k: v for k, v in attributes.items() if isinstance(v, DynamicVar)}


def has_dynamic_vars(attributes):
    """Returns True if any attribute value is a DynamicVar."""
    return any(isinstance(v, DynamicVar) for v in attributes.values())
