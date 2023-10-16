def pytest_addoption(parser):
    """
    The image on Artifactory that corresponds to the currently
    committed Metaflow version.
    """
    parser.addoption("--image", action="store", default=None)
    parser.addoption(
        "--opsgenie-api-token", dest="opsgenie_api_token", action="store", default=None
    )
    parser.addoption(
        "--pipeline-tag", dest="pipeline_tag", action="store", default=None
    )
