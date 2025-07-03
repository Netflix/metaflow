_UNINITIALIZED = object()
_info_file_content = _UNINITIALIZED


def read_info_file():
    # Prevent circular import
    from .packaging_sys import MetaflowCodeContent

    global _info_file_content

    if id(_info_file_content) == id(_UNINITIALIZED):
        _info_file_content = MetaflowCodeContent.get_info()
    return _info_file_content
