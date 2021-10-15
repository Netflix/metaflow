import re
from metaflow.plugins.aws.eks.kubernetes import sanitize_label_value, LABEL_VALUE_REGEX


def test_label_value_santitizer():
    assert LABEL_VALUE_REGEX.match(sanitize_label_value('HelloFlow'))

    # The value is too long
    assert LABEL_VALUE_REGEX.match(sanitize_label_value('a' * 1000))

    # Different long values should still not be equal after sanitization
    assert sanitize_label_value('a' * 1000) != sanitize_label_value('a' * 1001)
    assert sanitize_label_value('-' * 1000) != sanitize_label_value('-' * 1001)

    # Different long values should still not be equal after sanitization
    assert sanitize_label_value('alice!') != sanitize_label_value('alice?')

    # ends with dash
    assert LABEL_VALUE_REGEX.match(sanitize_label_value('HelloFlow-'))

    # non-ascii
    assert LABEL_VALUE_REGEX.match(sanitize_label_value('метафлоу'))

    # different only in case
    assert sanitize_label_value('Alice') != sanitize_label_value('alice')

    # spaces
    assert LABEL_VALUE_REGEX.match(sanitize_label_value('Meta flow'))