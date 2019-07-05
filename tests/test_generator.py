# pylint: disable=missing-docstring, no-self-use
import copy

import pytest

import table_properties as tp


@pytest.mark.skip(reason="Configs need to be updated")
class TestGenerator:
    def test_excalibur_increase_replicas(self, current_config):
        # Load the YAML
        desired_config = \
            tp.utils.load_yaml("./tests/configs/excalibur_incr_dcs.yaml")

        # Validate output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config),
            desired_config)

        expected_stmt = "ALTER KEYSPACE excalibur WITH replication = " \
            "{'class': 'NetworkTopologyStrategy', 'data_center1': 4, " \
            "'data_center2': 5};\n"
        assert stmt is not None
        assert stmt == expected_stmt

    def test_excalibur_unchanged(self, current_config):
        # Load the YAML
        desired_config = \
            tp.utils.load_yaml("./tests/configs/excalibur_unchanged.yaml")

        assert current_config == desired_config

        # Validate generator output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config),
            desired_config)

        assert isinstance(stmt, str)
        assert stmt == ""

    def test_excalibur_change_table_fields(self, current_config):
        # Load the YAML
        desired_config = \
            tp.utils.load_yaml("./tests/configs/excalibur_change_comments.yaml"
                               )

        # Validate output
        stmt = tp.generator.generate_alter_statements(
            copy.deepcopy(current_config),
            desired_config)

        expected_stmt = "\nUSE \"excalibur\";\nALTER TABLE monkeyspecies\n" \
            "WITH comment = 'Test comment';\nALTER TABLE monkeyspecies2\n" \
            "WITH comment = 'Test comment 2';"
        assert stmt is not None
        assert stmt != ""
        assert stmt == expected_stmt
