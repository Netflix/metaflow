"""This test modules implements unit tests for the
function`PluginHelper.sanitize_conda_package_url`.
"""


import unittest
from metaflow.plugins.conda.utils import PluginHelper  # noqa SC200

try:
    from urlparse import urlparse
except:  # noqa E722
    from urllib.parse import urlparse


class TestCondaChannelSanitizer(unittest.TestCase):  # noqa SC200
    """Unit test class."""

    def test_sanitize_urls(self):
        """Run unit-test."""
        input_array = [
            {"url": "", "expected_url": ""},
            {"url": None, "expected_url": None},
            {
                "url": "https://remote.private.com/channel1",
                "expected_url": "https://remote.private.com/channel1",
            },
            {
                "url": "https://remote.private.com:443/channel1",
                "expected_url": "https://remote.private.com:443/channel1",
            },
            {
                "url": "https://user:password@remote.private.com:443",
                "expected_url": "https://remote.private.com:443",
            },
            {
                "url": "https://user:password@remote.private.com:443/channel1",
                "expected_url": "https://remote.private.com:443/channel1",
            },
            {
                "url": "https://remote.private.com:443/t/token/channel1",
                "expected_url": "https://remote.private.com:443/channel1",
            },
            {
                "url": "https://remote.private.com:443/t/token/ch1/label/id1",
                "expected_url": "https://remote.private.com:443/ch1/label/id1",
            },
        ]
        for d in input_array:
            url = d.get("url")
            expected_url = d.get("expected_url")
            sanitized_url = PluginHelper.sanitize_conda_package_url(  # noqa SC200
                url
            )
            self.assertEqual(sanitized_url, expected_url)
            if url:
                sanitized_parsed_url = PluginHelper.sanitize_conda_package_url(  # noqa SC200
                    urlparse(url)
                )
                self.assertEqual(sanitized_parsed_url, urlparse(expected_url))


if __name__ == "__main__":
    unittest.main()
