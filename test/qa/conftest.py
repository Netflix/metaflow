import pytest
import uuid

# Add pytest helpers


def pytest_addoption(parser):
    parser.addoption(
        "--test-id",
        action="store",
        default=None,
        help="A unique ID for this test run. One will be generated if not provided",
    )


@pytest.fixture(scope="session")
def test_id(request):
    test_id = request.config.getoption("--test-id")

    if test_id is not None:
        return test_id

    # check for pytest-xdist availability
    if hasattr(request.config, "workerinput"):
        testrunid = request.config.workerinput["testrunuid"]
        return testrunid[:8]

    return uuid.uuid4().hex[:8]
