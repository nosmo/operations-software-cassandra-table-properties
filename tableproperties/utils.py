# pylint: disable = missing-docstring
""" Helper functions
"""
import logging
from typing import Any
import sys

DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_LEVEL = "CRITICAL"


def setup_logging(log_file: str = None, log_level: str = DEFAULT_LOG_LEVEL) -> None:
    """Apply log format and set level to root logger

    Args:
        log_file:  optional name of log file
        log_level: log level
    """
    hdlrs = [logging.StreamHandler(sys.stderr)]
    if log_file:
        hdlrs.append(logging.FileHandler(log_file))  # type: ignore

    log_level = getattr(logging, log_level.upper())

    logging.basicConfig(format=DEFAULT_LOG_FORMAT, handlers=hdlrs, level=log_level)


def find_by_value(dict_list: list, key: str, value, default_value=None) -> Any:
    """Search list of dictionaries by value and return first match

    Returns:
        First dictionary containing the value or default_value
    """
    if not isinstance(dict_list, list) or not key or not value:
        return default_value

    matches = list(
        filter(lambda x: isinstance(x, dict) and x.get(key) == value, dict_list)
    )

    return matches[0] if matches else default_value
