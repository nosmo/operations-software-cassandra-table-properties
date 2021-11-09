# pylint: disable=missing-docstring, no-self-use
import copy
import yaml

import tableproperties as tp


def load_yaml(filename: str):
    conf = None
    with open(filename, "r", encoding="utf-8") as conf_file:
        conf = yaml.safe_load(conf_file)

    return conf


def compare_statments(actual, expected):
    act_stmt_arr = actual.split(";")
    exp_stmt_arr = expected.split(";")
    assert len(act_stmt_arr) == len(exp_stmt_arr)
    if len(act_stmt_arr) == len(exp_stmt_arr):
        for act, exp in zip(act_stmt_arr, exp_stmt_arr):
            assert act == exp


class TestGenerator:
    def test_excalibur_increase_replicas(self, default_database):
        # Load the YAML
        desired_config = load_yaml("./tests/configs/excalibur_incr_dcs.yaml")

        current_config = default_database.get_current_config(True)

        # Validate output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config), desired_config
        )

        assert stmt is not None
        assert "'data_center1': 4" in stmt
        assert "'data_center2': 5" in stmt

    def test_excalibur_unchanged(self, default_database):
        # Load the YAML
        desired_config = load_yaml("./tests/configs/excalibur_unchanged.yaml")

        current_config = default_database.get_current_config(True)

        assert current_config == desired_config

        # Validate generator output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config), desired_config
        )

        assert isinstance(stmt, str)
        assert stmt == ""

    def test_excalibur_change_table_fields(self, default_database):
        # Load the YAML
        desired_config = load_yaml("./tests/configs/excalibur_change_comments.yaml")

        current_config = default_database.get_current_config(True)

        # Validate output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config), desired_config
        )

        expected_stmt = (
            '\nALTER TABLE "excalibur"."monkeyspecies"\n'
            "WITH comment = 'Test comment';"
            '\nALTER TABLE "excalibur"."monkeyspecies2"\n'
            "WITH comment = 'Test comment 2';"
        )
        assert stmt is not None
        assert stmt != ""
        compare_statments(stmt, expected_stmt)

    def test_class_name_comparision(self):
        assert tp.generator.do_class_names_match("SimpleStrategy", "SimpleStrategy")
        assert tp.generator.do_class_names_match(
            "org.apache.cassandra.locator.SimpleStrategy", "SimpleStrategy"
        )
        assert tp.generator.do_class_names_match(
            "org.apache.cassandra.locator.SimpleStrategy",
            "org.apache.cassandra.locator.SimpleStrategy",
        )
        assert not tp.generator.do_class_names_match(
            "org.apache.cassandra.locator.SimpleStrategy",
            "org.apache.cassandra.locator1.SimpleStrategy",
        )
        assert tp.generator.do_class_names_match("", "")
        assert tp.generator.do_class_names_match(None, None)
