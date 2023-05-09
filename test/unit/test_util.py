import os
import unittest
from unittest.mock import patch

from metaflow.util import get_username


class TestGetUsername(unittest.TestCase):
    @patch(
        "metaflow.metaflow_config.USER",
        "some-user",
    )
    def test_username_from_metaflow_config(self):
        self.assertEqual(
            "some-user",
            get_username(),
        )

    @patch.dict(
        os.environ,
        {
            "SUDO_USER": "some-sudo-user",
            "USERNAME": "some-other-username",
            "USER": "some-other-user",
        },
    )
    def test_username_from_sudouser_envvar(self):
        self.assertEqual(
            "some-sudo-user",
            get_username(),
        )

    @patch.dict(
        os.environ,
        {
            "SUDO_USER": "root",
            "USERNAME": "some-other-username",
            "USER": "some-other-user",
        },
    )
    def test_username_from_username_envvar(self):
        self.assertEqual(
            "some-other-username",
            get_username(),
        )

    @patch.dict(
        os.environ,
        {
            "SUDO_USER": "root",
            "USER": "some-other-user",
        },
    )
    def test_username_from_user_envvar(self):
        self.assertEqual(
            "some-other-user",
            get_username(),
        )


if __name__ == "__main__":
    unittest.main()
