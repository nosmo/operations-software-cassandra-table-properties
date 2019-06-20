import json
import logging

import table_properties as tp

def compare_values(src:dict, dst:dict) -> list:
    """ Compare configuration properties

    Args:
        src: Current values
        dst: Desired values

    Returns:
        List of changed values
    """
    def is_primitive(obj) -> bool:
        """Helper function for non-mapped properties

        Args:
            obj: Property value

        Returns:
            True if bool, string, integer, or float. False otherwise
        """
        return isinstance(obj, bool) or isinstance(obj, str) \
            or isinstance(obj, int) or isinstance(obj, float)

    if not (isinstance(src, dict) and isinstance(dst, dict)):
        return []

    changed_values = []
    for k in dst.keys():
        # Skip name properties
        if k == "name":
            continue

        dst_value = dst.get(k)
        src_value = src.get(k, None)
        if (src_value and not is_primitive(src_value)) \
            or not is_primitive(dst_value):
            logging.debug("""The value of '%s' is a mapped type.
                             You may want to break the type down.""", k)

        if src_value != dst_value:
            changed_values.append({ "property": k, "desired": dst_value })

    return changed_values

def connect_statments(stmt: str, add_new_line: bool = False) -> str:
    """Helper function to connect partial CQL statements.

    Args:
        stmt:         Current CQL statement
        add_new_line: Flag if connecting statement should be on new line

    Returns:
        connection line
    """
    spacer = "\n" if add_new_line else " "
    return spacer + "WITH " if not "WITH " in stmt else spacer + "AND "

def generate_alter_keyspace_statement(keyspace_name: str, \
    current_keyspace: dict, desired_keyspace: dict) -> str:
    """Create ALTER statements for keyspace changes.

    Args:
        keyspace_name:    Keyspace
        current_keyspace: Current keyspace properties
        desired_keyspace: Desired keyspace properties

    Returns:
        CQL statement with changed properties or empty string
    """
    current_replication = current_keyspace.pop("replication", {})
    desired_replication = desired_keyspace.pop("replication", {})

    stmt = "ALTER KEYSPACE {}".format(keyspace_name)

    changes = compare_values(current_keyspace, desired_keyspace)
    if changes:
        for change in changes:
            stmt += connect_statments(stmt)
            stmt += "{} = {}".format(change["property"], change["desired"])

    repl_changes = compare_values(current_replication, desired_replication)

    if repl_changes:
        stmt += connect_statments(stmt)
        stmt += "replication = {"

        repl_class = desired_replication.get("class", None)
        if not repl_class:
            raise ValueError("Replication class must be provided")
        
        stmt += "'class': '{}'".format(repl_class)

        if desired_replication.get("replication_factor", None):
            try:
                repl_factor = \
                    int(desired_replication.get("replication_factor"))
            except ValueError:
                repl_factor = 1
            
            stmt += ", 'replication_factor': {} ".format(repl_factor)

        desired_dcs = desired_replication.pop("data_centers", {})

        if "NetworkTopologyStrategy" == repl_class and desired_dcs:
            # Enumerate data center replication settings
            if isinstance(desired_dcs, list):
                for dc in desired_dcs:
                    stmt = stmt + ",'{}': '{}'" \
                        .format(dc["name"], dc["replication_factor"])

        stmt += "};\n"

    return stmt if "WITH " in stmt else ""

def generate_alter_table_statement(keyspace_name: str, \
    current_tables: dict, desired_tables: dict) -> str:
    """ Create ALTER statements for tables in keyspace

    Args:
        keyspace_name:  Keyspace name
        current_tables: Current table properties
        desired_tables: Desired table properties

    Returns:
        CQL statement with changed properties or empty string
    """
    tbl_stmts = "\nUSE \"{}\";".format(keyspace_name)

    for desired_table in desired_tables:
        tbl_name = desired_table.get("name", None)
        if not tbl_name:
            raise Exception("Missing table name in config")

        current_table = tp.utils.find_by_value(current_tables, "name", \
            tbl_name)

        if not current_table:
            logging.warning("Table '%s' does not exist. Skipping...", \
                tbl_name)

        changes = compare_values(current_table, desired_table)
        if changes:
            tbl_stmt = "\nALTER TABLE {}".format(tbl_name)
            for change in changes:
                prop_name = change["property"]
                prop_value = change["desired"]
                if not prop_value or prop_name == "id":
                    continue
                tbl_stmt += connect_statments(tbl_stmt, True)
                tbl_stmt += "{} = {}".format(prop_name, prop_value) \
                            if (isinstance(prop_value, dict) or \
                                str(prop_value).isnumeric()) \
                                else "{} = '{}'".format(prop_name, prop_value)
            tbl_stmts += tbl_stmt
            tbl_stmts += ";"

    return tbl_stmts if isinstance(tbl_stmts, str) and "WITH " in tbl_stmts \
        else ""

def generate_alter_statements(current_config: dict, desired_config: dict)->str:
    """ Create ALTER statements for tables and keyspaces

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
            logging.error("Invalid keyspace '%s' config. Missing 'name' key", 
                           desired_keyspace)
            raise Exception("Invalid configuration data")

        current_keyspace = tp.utils.find_by_value(current_keyspaces, "name",
                                                  ks_name)
        if not current_keyspace:
            logging.warning("""Skipped keyspace '%s'. Not found in 
                             current config. Add keyspace via DDL schema""",
                             ks_name)
            continue

        current_tables = current_keyspace.pop("tables", {})
        desired_tables = desired_keyspace.pop("tables", {})

        stmt += generate_alter_keyspace_statement(ks_name, \
            current_keyspace, desired_keyspace)

        stmt += generate_alter_table_statement(ks_name, current_tables, \
            desired_tables)

    return stmt

def main():
    # Load keyspaces properties and table properties from current instance
    current_config = tp.db.get_current_config()

    # Load desired keyspace and table properties
    desired_config = tp.utils.load_yaml("./configs/excalibur.yaml")

    generate_alter_statements(current_config, desired_config)

if __name__ == "__main__":
    main()