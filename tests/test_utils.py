import datetime
import logging
import os
import tempfile

import jsonschema
import pytest

import table_properties.utils as util

def get_temp_filename(prefix: str):
    return prefix + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

class TestFindByValue(object):
    def test_simple_list(self):
        l = [1, 2, 3, 4]
        assert util.find_by_value(l, "a", 1) is None

    def test_nonnested_dict_list(self):
       l = [ { "a": 1, "b": 2 }, { "c": 3, "d": 4 }, {}, { "e": 1 } ]
       assert isinstance(util.find_by_value(l, "a", 1), dict)
       assert util.find_by_value(l, "a", 1) == { "a": 1, "b": 2 }
       assert util.find_by_value(l, "f", 99) is None
       assert util.find_by_value(l, "d", 4) == { "c": 3, "d": 4 }
       assert util.find_by_value(l, "e", 1) == { "e": 1 }
       assert util.find_by_value(l, "e", None) is None

class TestInputOutput():
    def test_read_yaml_config(self):
        orig = util.load_yaml("./tests/configs/excalibur_unchanged.yaml")
        assert orig is not None
    def test_write_yaml_config(self):
        tmp_dir = tempfile.gettempdir()
        tmp_filename = os.path.join(tmp_dir,
            get_temp_filename(tempfile.gettempprefix()))

        orig = util.load_yaml("./tests/configs/excalibur_unchanged.yaml")
        assert util.write_yaml(tmp_filename, orig) == True
        assert orig == util.load_yaml(tmp_filename)

        # Remove temp file
        try:
            os.remove(tmp_filename)
        except Exception as ex:
            logging.error(ex)

    @pytest.mark.skip(reason="Validation no longer supported")
    def test_read_config_jsonschema(self):
        config_schema = util.load_json(("./schemas/config.json"))
        assert config_schema is not None
        config_data = util.load_yaml("./tests/configs/excalibur_unchanged.yaml")
        result = jsonschema.Draft7Validator(config_schema).is_valid(config_data)
        assert result == True

    def test_app_path(self):
        assert util.get_app_folder() == os.path.abspath(".")
