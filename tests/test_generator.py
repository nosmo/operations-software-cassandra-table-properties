# pylint: disable=missing-docstring, no-self-use
import copy

# import pytest

import table_properties as tp


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
        desired_config = \
            tp.utils.load_yaml("./tests/configs/excalibur_incr_dcs.yaml")

        current_config = default_database.get_current_config(True)

        # Validate output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config),
            desired_config)

        assert stmt is not None
        assert "'data_center1': 4" in stmt
        assert "'data_center2': 5" in stmt

    def test_excalibur_unchanged(self, default_database):
        # Load the YAML
        desired_config = \
            tp.utils.load_yaml("./tests/configs/excalibur_unchanged.yaml")

        current_config = default_database.get_current_config(True)

        assert current_config == desired_config

        # Validate generator output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config),
            desired_config)

        assert isinstance(stmt, str)
        assert stmt == ""

    def test_excalibur_change_table_fields(self, default_database):
        # Load the YAML
        desired_config = \
            tp.utils.load_yaml("./tests/configs/excalibur_change_comments.yaml"
                               )

        current_config = default_database.get_current_config(True)

        # Validate output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config),
            desired_config)

        expected_stmt = "\nUSE \"excalibur\";\nALTER TABLE monkeyspecies\n" \
            "WITH comment = 'Test comment';\nALTER TABLE monkeyspecies2\n" \
            "WITH comment = 'Test comment 2';"
        assert stmt is not None
        assert stmt != ""
        compare_statments(stmt, expected_stmt)
