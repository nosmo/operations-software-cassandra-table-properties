import json
import logging
import os
import yaml

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
