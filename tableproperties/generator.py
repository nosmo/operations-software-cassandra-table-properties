# pylint: disable=missing-docstring
""" Functions to create ALTER statements
"""
import logging
from typing import Any, List

from tableproperties import utils

#TODO integrate the logic of this class into the db class - we should be able
#to input and output changes as a result of being interfaces to the same data
#rather than representing them twice in two different methods.

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

    alter_statement = ""
    changes = compare_values(current_keyspace, desired_keyspace)

    if changes:
        prop_values = [
            "{} = {}".format(chg.get("property"), chg.get("desired")) for chg in changes
        ]
        assignments = "\nAND ".join(prop_values)
        alter_statement = 'ALTER KEYSPACE "{}" WITH {};'.format(keyspace_name, assignments)

    return alter_statement


def generate_alter_table_statement(
    keyspace_name: str, current_tables: list, desired_tables: list
    ) -> List[str]:
    """Create ALTER statements for tables in keyspace

    Args:
        keyspace_name:  Keyspace name
        current_tables: Current table properties
        desired_tables: Desired table properties

    Returns:
        List of CQL statements with changed properties
    """

    def format_value(val: Any) -> Any:
        return val if isinstance(val, dict) else "'" + str(val) + "'"

    table_statements = []
    for desired_table in desired_tables:
        table_name = desired_table.get("name", None)
        if not table_name:
            raise Exception("Missing table name in config")

        current_table = utils.find_by_value(current_tables, "name", table_name)

        if not current_table:
            logging.warning("Table '%s' does not exist. Skipping...", table_name)

        changes = compare_values(current_table, desired_table)
        if changes:
            prop_values = [
                "{} = {}".format(chg.get("property"), format_value(chg.get("desired")))
                for chg in changes
                if chg.get("desired") and chg.get("property") != "id"
            ]
            assignments = "\nAND ".join(prop_values)
            table_statement = '\nALTER TABLE "{}"."{}"\nWITH {};'.format(
                keyspace_name, table_name, assignments
            )
            table_statements.append(table_statement)

    return table_statements


def normalise_auth_resource(auth_resource: str):
    """Convert a Cassandra permissions table entry to CQL

    Args:
        auth_resource: A Cassandra resource string of the format "type/name[/subname]"

    Returns:
        A string of the form "TYPE normalised_name" suitable for use in CQL statement
    """
    resource_type, _, resource_selector = auth_resource.partition("/")
    if resource_type == "data" and resource_selector:
        if "/" in resource_selector: # resource is a table
            keyspace, table = resource_selector.split("/")
            normalised_resource = "TABLE \"{}\".\"{}\"".format(keyspace, table)
        else: # resource is a keyspace
            normalised_resource = "TABLE \"{}\"".format(resource_selector)
    #elif resource_type == "data" and not resource_selector:
    #    None #TODO support grants for "data" with no selector
    elif resource_type == "functions":
        normalised_resource = "FUNCTION \"{}\"".format(resource_selector)
    elif resource_type == "roles":
        normalised_resource = "ROLE \"{}\"".format(resource_selector)

    return normalised_resource


def generate_user_statements(current_config: dict, desired_config: dict,
                             annotate_statements: bool = False) -> List[str]:
    """Create ALTER statements for roles and permissions

    Args:
        current_config: Current properties
        desired_config: Desired properties
        annotate_statements: (optional) explain why a statement was generated #TODO

    Returns:
        List of CQL statements in order to reach the desired state
    """

    statements = []

    current_roles = current_config.get("roles", [])
    desired_roles = desired_config.get("roles", [])

    # Remove removed roles - create new roles later
    removed_roles = [i for i in current_roles if i not in desired_roles]
    for rm_role in removed_roles:
        statements.append("DROP ROLE IF EXISTS {};".format(rm_role))

    for role_elem in current_roles:
        if role_elem not in desired_roles:
            continue

        for role_attr in ["login", "superuser"]:
            if current_roles[role_elem][role_attr] != desired_roles[role_elem][role_attr]:
                statements.append("ALTER ROLE {} WITH {}={};".format(
                    role_elem,
                    role_attr.upper(),
                    "false" if not desired_roles[role_elem][role_attr] else "true"
                    ))

        for perm_elem in current_roles[role_elem]["permissions"]:

            normalised_resource = normalise_auth_resource(perm_elem)
            if perm_elem not in desired_roles[role_elem]["permissions"]:
                statements.append(
                    "REVOKE ALL ON {} FROM {};".format(normalised_resource, role_elem))
            else:
                removed_perms = [p for p in current_roles[role_elem]["permissions"][perm_elem] if p not in desired_roles[role_elem]["permissions"][perm_elem]]
                added_perms = [p for p in desired_roles[role_elem]["permissions"][perm_elem] if p not in current_roles[role_elem]["permissions"][perm_elem]]

                if removed_perms:
                    statements.append("REVOKE {} ON {} FROM {};".format(
                        ",".join(removed_perms),
                        normalised_resource,
                        role_elem
                    ))
                if added_perms:
                    statements.append("GRANT {} ON {} TO {};".format(
                        ",".join(added_perms),
                        normalised_resource,
                        role_elem
                    ))

    for new_role in [i for i in desired_roles if i not in current_roles]:
        statements.append("CREATE ROLE {} WITH SUPERUSER={} AND LOGIN={};".format(
            new_role,
            "true" if desired_roles[new_role]["superuser"] else "false",
            "true" if desired_roles[new_role]["login"] else "false",
        ))
        for new_perms, perm_list in desired_roles[new_role]["permissions"].items():
            normalised_resource = normalise_auth_resource(new_perms)
            statements.append("GRANT {} ON {} TO {};".format(
                ",".join(perm_list),
                normalised_resource,
                new_role
            ))

    return statements

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

    statements = []

    statements += generate_user_statements(current_config, desired_config)

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

        statements += generate_alter_keyspace_statement(
            ks_name, current_keyspace, desired_keyspace
        )

        statements += generate_alter_table_statement(ks_name, current_tables, desired_tables)

    return statements
