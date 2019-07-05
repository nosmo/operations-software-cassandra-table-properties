# pylint: disable=missing-docstring, invalid-name, no-self-use
import table_properties.db as db

# pylint: disable=too-few-public-methods
class TestDb():
    def test_ConvertValue(self):
        assert db.convert_value(1.0) == 1.0
        assert isinstance(db.convert_value((1.0)), float)
        assert db.convert_value(1) == 1
        assert isinstance(db.convert_value(1), int)
        assert db.convert_value("1") == 1
        assert isinstance(db.convert_value("1"), int)
        assert db.convert_value("2.0") == 2.0
        assert isinstance(db.convert_value("2.0"), float)
        assert db.convert_value("test") == "test"
        assert db.convert_value([1, 2]) == [1, 2]
