import table_properties.utils as util

class TestFlattenedDict(object):
    def test_empty_dict(self):
        fd = util.flatten_dict({},"","_")
        assert fd == {}

    def test_nonnested_dict(self):
        d = { "a": 1, "b": 2}
        assert util.flatten_dict(d) == d

    def test_nested_dict_with_sep(self):
        d = { 
                "a": 1,
                "b": 2,
                "c": {
                        "d": 3,
                        "e": { 
                                "f": [{"g": 1 }, {"h" : 2}]
                             }
                    }
            }
        fd = util.flatten_dict(d, "_")
        assert "a" in fd
        assert fd.get("a", 0) == 1
        assert "c_d" in fd
        assert "c_e_f" in fd
        assert isinstance(fd.get("c_e_f", None), list)

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