# Keep this file minimum dependency as this will be imported by metaflow at bootup.
def namedtuple_with_defaults(typename, field_descr, defaults=()):
    from typing import NamedTuple

    T = NamedTuple(typename, field_descr)
    T.__new__.__defaults__ = tuple(defaults)

    # Adding the following to ensure the named tuple can be (un)pickled correctly.
    import __main__

    setattr(__main__, T.__name__, T)
    T.__module__ = "__main__"
    return T


# Define the namedtuple with default here if they need to be accessible in client
# (and w/o a real flow).
foreach_frame_field_list = [
    ("step", str),
    ("var", str),
    ("num_splits", int),
    ("index", int),
    ("value", str),
]
ForeachFrame = namedtuple_with_defaults(
    "ForeachFrame", foreach_frame_field_list, (None,) * (len(foreach_frame_field_list))
)
