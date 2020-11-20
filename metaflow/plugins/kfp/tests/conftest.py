def pytest_addoption(parser):
    """
    The image on Artifactory that corresponds to the currently
    committed Metaflow version.
    """
    parser.addoption("--image", action="store", default=None)
