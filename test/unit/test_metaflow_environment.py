import unittest

from metaflow.metaflow_environment import MetaflowEnvironment


class _FakeFlow:
    """MetaflowEnvironment.__init__ ignores its flow arg for the local TYPE."""

    pass


def _env():
    return MetaflowEnvironment(_FakeFlow())


class TestGetInstallDependenciesCmd(unittest.TestCase):
    def test_s3_includes_expected_packages(self):
        cmd = _env()._get_install_dependencies_cmd("s3")
        self.assertIn("boto3", cmd)
        self.assertIn("requests", cmd)

    def test_azure_includes_expected_packages(self):
        cmd = _env()._get_install_dependencies_cmd("azure")
        for pkg in ["azure-identity", "azure-storage-blob", "requests"]:
            self.assertIn(pkg, cmd)

    def test_gs_includes_expected_packages(self):
        cmd = _env()._get_install_dependencies_cmd("gs")
        for pkg in ["google-cloud-storage", "google-auth", "requests"]:
            self.assertIn(pkg, cmd)

    def test_unknown_datastore_raises(self):
        with self.assertRaises(NotImplementedError):
            _env()._get_install_dependencies_cmd("unknown")

    def test_skip_install_dependencies_guard(self):
        cmd = _env()._get_install_dependencies_cmd("s3")
        self.assertIn("METAFLOW_SKIP_INSTALL_DEPENDENCIES", cmd)
        self.assertTrue(cmd.startswith("if ["))
        self.assertTrue(cmd.rstrip().endswith("fi"))

    def test_pip_check_before_install(self):
        cmd = _env()._get_install_dependencies_cmd("s3")
        self.assertIn("pip --version", cmd)
        self.assertIn("ensurepip", cmd)
        self.assertLess(cmd.find("pip --version"), cmd.find("pip install"))


if __name__ == "__main__":
    unittest.main()
