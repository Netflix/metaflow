from metaflow.exception import MetaflowTaggingError
from metaflow.util import unicode_type, bytes_type


def is_utf8_encodable(x):
    """
    Returns true if the object can be encoded with UTF-8
    """
    try:
        x.encode("utf-8")
        return True
    except UnicodeError:
        return False


def is_utf8_decodable(x):
    """
    Returns true if the object can be decoded with UTF-8
    """
    try:
        x.decode("utf-8")
        return True
    except UnicodeError:
        return False


# How many user tags are allowed on a run
MAX_USER_TAG_SET_SIZE = 50
# How long may an individual tag value be
MAX_TAG_SIZE = 500


def validate_tags(tags, existing_tags=None):
    """
    Raises MetaflowTaggingError if invalid based on these rules:

    Tag set size is too large. But it's OK if tag set is not larger
    than an existing tag set (if provided).

    Then, we validate each tag.  See validate_tag()
    """
    tag_set = frozenset(tags)
    if len(tag_set) > MAX_USER_TAG_SET_SIZE:
        # We want to allow user to remediate excessively large tag sets via tag mutation
        if existing_tags is None or len(frozenset(existing_tags)) < len(tag_set):
            raise MetaflowTaggingError(
                msg="Cannot increase size of tag set beyond %d"
                % (MAX_USER_TAG_SET_SIZE,)
            )
    for tag in tag_set:
        validate_tag(tag)


def validate_tag(tag):
    """
    - Tag must be either of bytes-type or unicode-type.
    - If tag is of bytes-type, it must be UTF-8 decodable
    - If tag is of unicode-type, it must be UTF-8 encodable
    - Tag may not be empty string.
    - Tag cannot be too long (500 chars)
    """
    if isinstance(tag, bytes_type):
        if not is_utf8_decodable(tag):
            raise MetaflowTaggingError("Tags must be UTF-8 decodable")
    elif isinstance(tag, unicode_type):
        if not is_utf8_encodable(tag):
            raise MetaflowTaggingError("Tags must be UTF-8 encodable")
    else:
        raise MetaflowTaggingError(
            "Tags must be some kind of string (bytes or unicode), got %s",
            str(type(tag)),
        )
    if not len(tag):
        raise MetaflowTaggingError("Tags must not be empty string")
    if len(tag) > MAX_TAG_SIZE:
        raise MetaflowTaggingError("Tag is too long %d > %d" % (len(tag), MAX_TAG_SIZE))
