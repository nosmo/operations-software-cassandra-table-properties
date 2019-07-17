# pylint: disable=missing-docstring, broad-except, invalid-name, no-self-use
import datetime

import pytest

import table_properties as tp


def get_temp_filename(prefix: str):
    return prefix + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


class TestFindByValue():
    def test_simple_list(self):
        l = [1, 2, 3, 4]
        assert tp.utils.find_by_value(l, "a", 1) is None

    def test_nonnested_dict_list(self):
        l = [{"a": 1, "b": 2}, {"c": 3, "d": 4}, {}, {"e": 1}]
        assert isinstance(tp.utils.find_by_value(l, "a", 1), dict)
        assert tp.utils.find_by_value(l, "a", 1) == {"a": 1, "b": 2}
        assert tp.utils.find_by_value(l, "f", 99) is None
        assert tp.utils.find_by_value(l, "d", 4) == {"c": 3, "d": 4}
        assert tp.utils.find_by_value(l, "e", 1) == {"e": 1}
        assert tp.utils.find_by_value(l, "e", None) is None


class TestInputOutput():
    def test_read_yaml_config(self):
        orig = tp.utils.load_yaml("./tests/configs/excalibur_unchanged.yaml")
        assert orig is not None

    def test_read_nonexisting_file(self):
        with pytest.raises(Exception):
            tp.utils.load_file("testing123", None)
