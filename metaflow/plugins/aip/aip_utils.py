import logging
import sys


def _get_aip_logger():
    """Setup logger for AIP plugin

    With expected usage from Jupyter notebook and console in mind,
    INFO level logs need to show up in Jupyter notebook output cell and consoles.
    Therefore some default setting below:
        - Default logging level at logging.DEBUG
        - Default to a StreamHandler pointing to stdout.
    Users retain the ability to alter logger behavior using logging module.
    """
    aip_logger = logging.getLogger("metaflow.aip")
    if not aip_logger.hasHandlers():
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        aip_logger.addHandler(stdout_handler)
    aip_logger.setLevel(logging.DEBUG)
    return aip_logger


logger = _get_aip_logger()
