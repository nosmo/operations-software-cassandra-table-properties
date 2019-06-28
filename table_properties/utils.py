#pylint: disable = missing-docstring, broad-except
""" Helper functions
"""
import json
import logging
import os
import time
import typing

import jsonschema
import yaml


DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

def setup_logging(log_file: str = "", log_level=logging.DEBUG) -> None:
    """Apply log format, set level, and add color to root logger

    Args:
        log_file:  optional name of log file
        log_level: log level defaults to DEBUG
    """
    log_file = os.path.join(get_app_folder(), log_file if log_file else \
        get_timestamped_filename("tp", ".log"))

    logging.basicConfig(format=DEFAULT_LOG_FORMAT,
                        handlers=[logging.FileHandler(log_file)],
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

def get_timestamped_filename(prefix: str = "", ext: str = "") -> str:
    """ Create a timestamped filename with custom prefix and
        extension

    Args:
        prefix: Prepended to the time stamp
        ext:    extension to be used

    Returns:
        Filename
    """
    new_prefix = prefix + "_" if prefix else ""
    new_ext = ext if "." in ext else "." + ext
    return "{}{}{}".format(new_prefix, time.strftime("%Y%m%d-%H%M%S"), new_ext)

def get_app_folder() -> str:
    """Return application folder path

    Returns:
        Path to app folder
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))

    return os.path.abspath(os.path.join(script_dir, os.pardir))

def load_file(filename: str, loader, encoding: str = "utf-8"):
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

    if not os.path.isfile(filename):
        msg = "File '{}' not found".format(filename)
        logging.error(msg)
        raise Exception(msg)

    try:
        with open(filename, "r", encoding=encoding) as in_file:
            content = loader(in_file)
    except Exception as ex:
        logging.exception(ex)

    return content

def write_file(filename: str, data: dict, overwrite: bool, writer,
               encoding: str = "utf-8", **options) -> bool:
    """Write a structured text file

    Args:
        filename: Full path to file
        writer:   Structured text write function, e.g. yaml.dump

    Returns:
        True if write succeeded. False otherwise
    """
    if os.path.isfile(filename) and not overwrite:
        msg = "Configuration file '{}' already exists.".format(filename) \
            + "Add -f or --force if you like to overwrite the existing file."
        logging.warning(msg)
        print(msg)

        return False

    try:
        with open(filename, "w", encoding=encoding) as outfile:
            writer(data, outfile, **options)
        return True
    except Exception as ex:
        logging.exception(ex)

    return False

def load_yaml(filename: str) -> typing.Optional[dict]:
    """Load a YAML config file

    Args:
        filename: Full path to file
    Returns:
        A dict or None.
    """
    return load_file(filename, yaml.safe_load)


def write_yaml(filename: str, data: dict, overwrite: bool = False) -> bool:
    """Write a YAML config file

    Args:
        filename: Full path to file
        data: yaml data to be written
    Returns:
        True if succeeded. False otherwise
    """
    return write_file(filename, data, overwrite, yaml.dump,
                      default_flow_style=False)

def load_json(filename: str) -> typing.Optional[dict]:
    """Load a JSON file.

    Args:
        filename: Full path to file
    Returns:
        A dict or None.
    """
    return load_file(filename, json.load)

def validate_schema(schema: dict, data: dict) -> bool:
    """Validate data against jsonschema.

    Args:
        schema: JSON Schema definition
        data: JSON data to be validated

    Returns:
        True if valid. False otherwise.
    """
    validator = None
    try:
        validator = jsonschema.Draft7Validator(schema)
        validator.validate(data)
        return True
    except jsonschema.ValidationError as val_err:
        logging.exception(val_err)
        for error in sorted(validator.iter_errors(data), key=str):
            logging.debug(error.message)
        raise Exception("Schema validation failed. See log for details.")

    return False

def find_by_value(dict_list: list, key: str, value, default_value=None):
    """Search list of dictionaries by value and return first match

    Returns:
        First dictionary containing the value or default_value
    """
    if not isinstance(dict_list, list):
        return default_value

    if not value:
        logging.warning("find_by_value() requires a value to be provided")
        return default_value

    for list_item in dict_list:
        if not isinstance(list_item, dict):
            continue
        if list_item.get(key, "") == value:
            return list_item

    return default_value
