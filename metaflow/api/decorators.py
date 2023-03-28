import metaflow as mf
from metaflow.meta import FOREACH, IFF, IFN, IS_STEP, JOIN, META_KEY, PREV_STEP


def get_meta(f, default=None):
    """Get (and set to empty dict if absent) a step function's metadata object."""
    if hasattr(f, META_KEY):
        return getattr(f, META_KEY)
    else:
        meta = default or {}
        setattr(f, META_KEY, meta)
        return meta


def step(arg):
    """Alternate "step" decorator; recognizable by `metaflow.api.Flow` metaclass.

    Also usable as a drop-in replacement for the usual `metaflow.step` decorator.

    :param arg: can be the decoratee itself (i.e. `@step` with no arguments, applied directly to a function, in which
                case the previous step in the class is inferred to precede this step in the flow graph) or can
                explicitly specify a previous step name (as a string)
    """

    def make_step(f, prev):
        meta = get_meta(f)
        meta[IS_STEP] = True
        meta[PREV_STEP] = prev

        # Ensure Backwards-compatibility with existing `@step` decorator
        return mf.step(f)

    if isinstance(arg, str):
        # `@step('<previous step name>')` form
        return lambda f: make_step(f, arg)
    elif callable(arg):
        # `@step` form
        return make_step(arg, None)
    else:
        raise ValueError(
            "@step must be passed a string step name or called directly on a method"
        )


def foreach(field, prev=None):
    """@foreach decorator: tag a step as operating on every element in a collection.

    :param field: attr holding a collection of objects to operate on
    :param prev: optional, explicitly specify this step's predecessor (likely the step that sets the `field` attr); by
                 default, the previous step in the class is used
    """

    def make_foreach(f, prev, field):
        meta = get_meta(f)
        meta[FOREACH] = dict(prev=prev, field=field)
        meta[IS_STEP] = True

        # Ensure Backwards-compatibility with existing `@step` decorator
        return mf.step(f)

    if not isinstance(field, str):
        raise ValueError(
            "`field` param to `@foreach` must be a string name of an instance var"
        )

    return lambda f: make_foreach(f, prev, field)


def join(*args):
    """@join decorator: tag a step as joining previous, branched steps.

    Steps to join can be passed explicitly as strings (`@join('step1', 'step2', ...)`) or omitted (`@join`; steps to
    join are inferred from graph)."""

    def make_join(f, steps):
        meta = get_meta(f)
        meta[JOIN] = steps
        meta[IS_STEP] = True

        # Ensure Backwards-compatibility with existing `@step` decorator
        return mf.step(f)

    def usage():
        raise ValueError(
            "@join must be called directly on a step function, or passed 1 or more step-names to join"
        )

    if args and all(isinstance(arg, str) for arg in args):
        """Support usage like `@join('key')`"""
        return lambda f: make_join(f, args)
    else:
        if len(args) == 1:
            # Support direct, no-arg `@join` decoration (applies to the last step)
            arg = args[0]
            if not callable(arg):
                usage()
            return make_join(arg, None)
        else:
            usage()


def iff(*args):
    if len(args) == 1:
        step, key = None, args[0]
    elif len(args) == 2:
        step, key = args
    else:
        raise ValueError("Expected 1 or 2 args to iff()")

    def make_iff(f):
        meta = get_meta(f)
        meta[IFF] = dict(step=step, key=key)
        meta[IS_STEP] = True

        # Ensure Backwards-compatibility with existing `@step` decorator
        return mf.step(f)

    return make_iff


def ifn(*args):
    if len(args) == 1:
        step, key = None, args[0]
    elif len(args) == 2:
        step, key = args
    else:
        raise ValueError("Expected 1 or 2 args to ifn()")

    def make_ifn(f):
        meta = get_meta(f)
        meta[IFN] = dict(step=step, key=key)
        meta[IS_STEP] = True

        # Ensure Backwards-compatibility with existing `@step` decorator
        return mf.step(f)

    return make_ifn
