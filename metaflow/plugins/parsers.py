from metaflow._vendor import yaml


def yaml_parser(content: str):
    """
    Parse YAML content to a dictionary.

    Parameters
    ----------
    content : str

    Returns
    -------
    dict
    """
    return yaml.safe_load(content)
