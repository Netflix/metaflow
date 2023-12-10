import pytest

from metaflow.plugins.pypi.pip import verify_wheel_tags_supported


@pytest.mark.parametrize(
    "wheel_name,python,platform,expected",
    [
        ("test-1.0-py3-none-any.whl", "3.12", "osx-64", True),
        ("test-1.0-py3-none-any.whl", "3.12", "osx-arm64", True),
        ("test-1.0-py3-none-any.whl", "3.12", "linux-64", True),
        ("test-1.0-py3-none-any.whl", "2.5", "linux-64", False),
        ("test-1.0-py2.py3-none-any.whl", "3.12", "linux-64", True),
        ("test-1.0-py2.py3-none-any.whl", "2.5", "linux-64", True),
        ("test-1.0-py3-none-linux_x86_64.whl", "3.11", "linux-64", True),
        ("test-1.0-py311-none-manylinux_2_24_x86_64.whl", "3.11", "linux-64", True),
        ("test-1.0-py311-none-manylinux_2_24_x86_64.whl", "3.11", "osx-64", False),
        ("test-1.0-py311-none-manylinux_2_24_x86_64.whl", "3.10", "linux-64", False),
        ("test-1.0-py2.py3-none-any.whl", "3.12", "osx-64", True),
        ("test-1.0-py2.py3-none-any.whl", "2.5", "osx-64", True),
        ("test-1.0-cp39.cp38-none-any.whl", "3.8", "osx-64", True),
        ("test-1.0-cp39.cp38-none-any.whl", "3.8", "osx-arm64", True),
        ("test-1.0-cp38-cp38-macosx_10_9_universal.whl", "3.8", "osx-64", True),
        ("test-1.0-cp38-cp38-macosx_10_9_universal2.whl", "3.8", "osx-arm64", True),
        ("test-1.0-cp39-cp39-macosx_10_12_universal2.whl", "3.9", "osx-arm64", True),
        ("test-1.0-cp39-cp39-macosx_10_12_universal2.whl", "3.9", "osx-64", True),
        ("test-1.0-cp39-cp39-macosx_13_0_universal2.whl", "3.9", "osx-arm64", True),
        ("test-1.0-cp39-cp39-macosx_13_0_universal2.whl", "3.8", "osx-arm64", False),
        ("test-1.0-cp311-cp311-macosx_13_0_arm64.whl", "3.11", "osx-arm64", True),
        ("test-1.0-cp311-cp311-macosx_13_0_universal2.whl", "3.11", "linux-64", False),
        ("test-1.0-cp311-cp311-macosx_13_0_arm64.whl", "3.11", "linux-64", False),
        ("test-1.0-cp311-cp311-macosx_13_0_x86_64.whl", "3.11", "linux-64", False),
    ],
)
def test_verify_wheel_tags_supported(wheel_name, python, platform, expected):
    assert verify_wheel_tags_supported(wheel_name, python, platform) == expected
