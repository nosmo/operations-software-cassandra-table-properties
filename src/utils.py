# pylint: disable = missing-docstring, broad-except
""" Helper functions
"""
import json
import logging
import sys
import typing

import yaml

DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_LEVEL = "critical"


def get_log_level(level: str):
    """Convert log level string to logging constant.

    Args:
        level: log level string
    Returns:
        Matching log level constant or default.
    """
    levels = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "warn": logging.WARNING,
        "error": logging.ERROR,
        "fatal": logging.FATAL,
        "critical": logging.CRITICAL
    }

    if not level or not isinstance(level, str):
        return levels[DEFAULT_LOG_LEVEL]

    lwr_level = level.lower()
    if lwr_level not in levels:
        return levels[DEFAULT_LOG_LEVEL]

    return levels[lwr_level]


def setup_logging(log_file: str = None, log_level=None) -> None:
    """Apply log format, set level, and add color to root logger

    Args:
        log_file:  optional name of log file
        log_level: log level
    """
    hdlrs = []
    if log_file:
        hdlrs.append(logging.FileHandler(log_file))
    else:
        hdlrs.append(logging.StreamHandler(sys.stderr))

    if not log_level:
        log_level = get_log_level(DEFAULT_LOG_LEVEL)

    logging.basicConfig(format=DEFAULT_LOG_FORMAT,
                        handlers=hdlrs,
                        level=log_level)

    # Color codes http://www.tldp.org/HOWTO/Bash-Prompt-HOWTO/x329.html
    logging.addLevelName(  # cyan
        logging.DEBUG, "\033[36m%s\033[0m" %
        logging.getLevelName(logging.DEBUG))
    logging.addLevelName(  # green
        logging.INFO, "\033[32m%s\033[0m" %
        logging.getLevelName(logging.INFO))
    logging.addLevelName(  # yellow
        logging.WARNING, "\033[33m%s\033[0m" %
        logging.getLevelName(logging.WARNING))
    logging.addLevelName(  # red
        logging.ERROR, "\033[31m%s\033[0m" %
        logging.getLevelName(logging.ERROR))
    logging.addLevelName(  # red background
        logging.CRITICAL, "\033[41m%s\033[0m" %
        logging.getLevelName(logging.CRITICAL))


def load_file(filename: str, loader, encoding: str):
    """Load a structured text file

    Retrieve a structured file in e.g. JSON or YAML format.

    Args:
        filename: Full path to file
        loader:   Structured text loader function, e.g. yaml.safe_load
        encoding: file encoding. Defaults to UTF-8

    Returns:
        A dict with the content or None if the file does not exist or
        cannot be read.
    """
    content = None

    if not loader:
        raise Exception("Missing function argument.")

    with open(filename, "r", encoding=encoding) as in_file:
        content = loader(in_file)

    return content


def load_yaml(filename: str, encoding: str = "utf-8") -> typing.Optional[dict]:
    """Load a YAML config file

    Args:
        filename: Full path to file
    Returns:
        A dict or None.
    """
    return load_file(filename, yaml.safe_load, encoding)


def load_json(filename: str, encoding: str = "utf-8") -> typing.Optional[dict]:
    """Load a JSON file.

    Args:
        filename: Full path to file
    Returns:
        A dict or None.
    """
    return load_file(filename, json.load, encoding)


def find_by_value(dict_list: list, key: str, value, default_value=None):
    """Search list of dictionaries by value and return first match

    Returns:
        First dictionary containing the value or default_value
    """
    if not isinstance(dict_list, list) or not key or not value:
        return default_value

    matches = list(filter(lambda x: isinstance(x, dict) and
                          x.get(key) == value, dict_list))

    return matches[0] if matches else default_value
