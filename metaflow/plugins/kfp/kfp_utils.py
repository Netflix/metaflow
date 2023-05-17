import logging
import sys


def _get_kfp_logger():
    """Setup logger for KFP plugin

    With expected usage from Jupyter notebook and console in mind,
    INFO level logs need to show up in Jupyter notebook output cell and consoles.
    Therefore some default setting below:
        - Default logging level at logging.DEBUG
        - Default to a StreamHandler pointing to stdout.
    Users retain the ability to alter logger behavior using logging module.
    """
    kfp_logger = logging.getLogger("metaflow.kfp")
    if not kfp_logger.hasHandlers():
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        kfp_logger.addHandler(stdout_handler)
    kfp_logger.setLevel(logging.DEBUG)
    return kfp_logger


logger = _get_kfp_logger()
