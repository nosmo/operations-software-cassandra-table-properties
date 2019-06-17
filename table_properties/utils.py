import functools
import json
import logging
import os
import yaml

DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

def setup_logging():
    logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=logging.INFO)

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

def get_app_folder():
    script_dir = os.path.dirname(os.path.realpath(__file__))

    return os.path.abspath(os.path.join(script_dir, os.pardir))

def load_file(filename, loader):
    content = None

    if not os.path.exists(filename):
        return content

    try:
        with open(filename) as f:
            content = loader(f)
    except Exception as ex:
        logging.error(ex)

    return content

def load_config(config_filename):
    return load_file(config_filename, yaml.safe_load)

def load_schema(schema_filename):
    return load_file(schema_filename, json.load)

def flatten_dict(d: dict, separator: str ='_', prefix: str =''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in d.items()
             for k, v in flatten_dict(vv, separator, kk).items()
           } if isinstance(d, dict) else { prefix : d }

def find_by_value(l: list, key: str, value, default_value = None):
    if not l or not isinstance(l, list):
        return default_value

    if not value:
        logging.warning("find_by_value() requires a value to be provided")
        return default_value

    for list_item in l:
        if not isinstance(list_item, dict):
            continue
        if list_item.get(key, "") == value:
            return list_item

    return default_value
