# pylint: disable=missing-docstring
""" Functions to create ALTER statements
"""
import logging
from typing import Any

from tableproperties import utils


def do_class_names_match(src_class: str, dst_class: str) -> bool:
    if src_class and dst_class:
        src_class_arr = src_class.split(".")
        dst_class_arr = dst_class.split(".")
        if len(src_class_arr) == len(dst_class_arr):
            return sorted(src_class_arr) == sorted(dst_class_arr)

        return src_class_arr[-1] == dst_class_arr[-1]
    if src_class or dst_class:
        # Only one class name was provided
        return False

    # Neither class names were provided
    return True


def compare_values(src: dict, dst: dict) -> list:
    """Compare configuration properties

    Args:
        src: Current values
        dst: Desired values

    Returns:
        List of changed values
    """

    changed_values = []
    for key in dst.keys():
        # Skip name properties
        if key == "name":
            continue
        dst_value = dst.get(key)
        src_value = src.get(key)
        is_same_class = True
        if src_value and isinstance(src_value, dict) and isinstance(dst_value, dict):
            src_class = src_value.pop("class", None)
            dst_class = dst_value.pop("class", None)
            is_same_class = do_class_names_match(src_class, dst_class)
        else:
            src_class = None
            dst_class = None

        if src_value != dst_value or not is_same_class:
            # Pop the class back in (changed or not)
            if dst_class or src_class:
                # pylint: disable=line-too-long
                dst_value["class"] = (  # type: ignore
                    dst_class if dst_class else src_class
                )
                # pylint: enable=line-too-long
            changed_values.append({"property": key, "desired": dst_value})

    return changed_values


def generate_alter_keyspace_statement(
    keyspace_name: str, current_keyspace: dict, desired_keyspace: dict
    ) -> str:
    """Create ALTER statements for keyspace changes.

    Args:
        keyspace_name:    Keyspace
        current_keyspace: Current keyspace properties
        desired_keyspace: Desired keyspace properties

    Returns:
        CQL statement with changed properties or empty string
    """

    alter_stmt = ""
    changes = compare_values(current_keyspace, desired_keyspace)

    if changes:
        prop_values = [
            "{} = {}".format(chg.get("property"), chg.get("desired")) for chg in changes
        ]
        assignments = "\nAND ".join(prop_values)
        alter_stmt = 'ALTER KEYSPACE "{}" WITH {};'.format(keyspace_name, assignments)

    return alter_stmt


def generate_alter_table_statement(
    keyspace_name: str, current_tables: list, desired_tables: list
    ) -> str:
    """Create ALTER statements for tables in keyspace

    Args:
        keyspace_name:  Keyspace name
        current_tables: Current table properties
        desired_tables: Desired table properties

    Returns:
        CQL statement with changed properties or empty string
    """

    def format_value(val: Any) -> Any:
        return val if isinstance(val, dict) else "'" + str(val) + "'"

    tbl_stmts = ""
    for desired_table in desired_tables:
        tbl_name = desired_table.get("name", None)
        if not tbl_name:
            raise Exception("Missing table name in config")

        current_table = utils.find_by_value(current_tables, "name", tbl_name)

        if not current_table:
            logging.warning("Table '%s' does not exist. Skipping...", tbl_name)

        changes = compare_values(current_table, desired_table)
        if changes:
            prop_values = [
                "{} = {}".format(chg.get("property"), format_value(chg.get("desired")))
                for chg in changes
                if chg.get("desired") and chg.get("property") != "id"
            ]
            assignments = "\nAND ".join(prop_values)
            tbl_stmt = '\nALTER TABLE "{}"."{}"\nWITH {};'.format(
                keyspace_name, tbl_name, assignments
            )
            tbl_stmts += tbl_stmt

    return tbl_stmts


def generate_alter_statements(current_config: dict, desired_config: dict) -> str:
    """Create ALTER statements for tables and keyspaces

    Args:
        current_config: Current properties
        desired_config: Desired properties

    Returns:
        CQL statement with changed properties or empty string
    """
    current_keyspaces = current_config.get("keyspaces", [])
    desired_keyspaces = desired_config.get("keyspaces", [])

    stmt = ""

    for desired_keyspace in desired_keyspaces:
        ks_name = desired_keyspace.get("name", None)
        if not ks_name:
            logging.error(
                "Invalid keyspace '%s' config. Missing 'name' key", desired_keyspace
            )
            raise KeyError("Invalid YAML conf. Missing keyspace name")

        current_keyspace = utils.find_by_value(current_keyspaces, "name", ks_name)
        if not current_keyspace:
            logging.warning(
                """Skipped keyspace '%s'. Not found in
                            current config. Add keyspace via DDL schema""",
                ks_name,
            )
            continue

        current_tables = current_keyspace.pop("tables", {})
        desired_tables = desired_keyspace.pop("tables", {})

        stmt += generate_alter_keyspace_statement(
            ks_name, current_keyspace, desired_keyspace
        )

        stmt += generate_alter_table_statement(ks_name, current_tables, desired_tables)

    return stmt
