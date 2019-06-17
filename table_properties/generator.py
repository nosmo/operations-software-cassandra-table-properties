import logging

import table_properties as tp

def isPrimitive(obj):
    if isinstance(obj, bool) or isinstance(obj, str) or isinstance(obj, int) \
        or isinstance(obj, float):
        return True

    return False

def compareValues(src:dict, dst:dict):
    if not (isinstance(src, dict) and isinstance(dst, dict)):
        return []

    changed_values = []
    for k in dst.keys():
        if k == "name":
            continue

        dst_value = dst.get(k)
        src_value = src.get(k, None)
        if (src_value and not isPrimitive(src_value)) or not isPrimitive(dst_value):
            logging.warning("The value of '%s' is a complex type. You may want to break the type down.", k)

        if not src_value:
            changed_values.append({ "action": "add" , "property": k, "desired": dst_value})
        elif src_value != dst_value:
            changed_values.append({ "action": "update" , "property": k, "current": src_value, "desired": dst_value})

    return changed_values

def connect_statments(stmt, add_new_line = False):
    spacer = "\n" if add_new_line else " "
    return f"{spacer}WITH " if not "WITH" in stmt else f"{spacer}AND "

def generate_alter_keyspace_statement(keyspace_name, current_keyspace, \
    desired_keyspace):

    current_replication = current_keyspace.pop("replication", {})
    desired_replication = desired_keyspace.pop("replication", {})

    # Just pop it to get rid of it
    current_replication.pop("data_centers", {})
    desired_dcs = desired_replication.pop("data_centers", {})

    stmt = f"ALTER KEYSPACE {keyspace_name}"

    changes = compareValues(current_keyspace, desired_keyspace)
    if changes:
        for change in changes:
            if change.get("action") == "update":
                stmt += connect_statments(stmt)
                stmt += "{} = {}".format(change["property"], change["desired"])

    repl_changes = compareValues(current_replication, desired_replication)

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

        if "NetworkTopologyStrategy" == repl_class and desired_dcs:
            # Enumerate data center replication settings
            if isinstance(desired_dcs, list):
                for dc in desired_dcs:
                    stmt = stmt + ",'{}': '{}'" \
                        .format(dc["name"], dc["replication_factor"])

        stmt += "};\n"

    return stmt if "WITH" in stmt else ""

def generate_alter_table_statement(keyspace_name, current_tables, \
    desired_tables):

    for desired_table in desired_tables:
        tbl_name = desired_table.get("name", None)
        if not tbl_name:
            raise Exception("Missing table name in config")

        current_table = tp.utils.find_by_value(current_tables, "name", \
            tbl_name)
        if not current_table:
            logging.warning("Table %s does not exist. Skipping...", \
                tbl_name)

        desired_caching = desired_table.pop("caching", {})
        current_caching = current_table.pop("caching", {})

        desired_cdc = desired_table.pop("cdc", {})
        current_cdc = current_table.pop("cdc", {})

        desired_compaction = desired_table.pop("compaction", {})
        current_compaction = current_table.pop("compaction", {})

        desired_compression = desired_table.pop("compression", {})
        current_compression = current_table.pop("compression", {})

        desired_extension = desired_table.pop("extensions", {})
        current_extension = current_table.pop("extensions", {})

        desired_flags = desired_table.pop("flags", {})
        current_flags = current_table.pop("flags", {})

        changes = compareValues(current_table, desired_table)
        if changes:
            tbl_stmt = f"USE {keyspace_name};\nALTER TABLE {tbl_name}"
            for change in changes:
                prop_name = change["property"]
                prop_value = change["desired"]
                if not prop_value or prop_name == "id":
                    continue
                tbl_stmt += connect_statments(tbl_stmt, True)
                tbl_stmt += "{} = {}" \
                    if (str(prop_value).isnumeric()) else "{} = '{}'"\
                        .format(prop_name, prop_value)
    
        caching_changes = compareValues(current_caching, desired_caching)
        if caching_changes:
            tbl_stmt += connect_statments(tbl_stmt, True)
            tbl_stmt += "caching = {}".format(desired_caching)

        cdc_changes = compareValues(current_cdc, desired_cdc)
        if cdc_changes:
            tbl_stmt += connect_statments(tbl_stmt, True)
            tbl_stmt += "cdc = {}".format(desired_cdc)

        compaction_changes = compareValues(current_compaction, \
            desired_compaction)

        if compaction_changes:
            tbl_stmt += connect_statments(tbl_stmt, True)
            tbl_stmt += "compaction = {}".format(desired_compaction)

        compression_changes = compareValues(current_compression, \
            desired_compression)

        if compression_changes:
            tbl_stmt += connect_statments(tbl_stmt, True)
            tbl_stmt += "compresion = {}".format(desired_compression)

        extension_changes = compareValues(current_extension, \
            desired_extension)

        if extension_changes:
            tbl_stmt += connect_statments(tbl_stmt, True)
            tbl_stmt += "extension = {}".format(desired_extension)

        flags_changes = compareValues(current_flags, \
            desired_flags)

        if flags_changes:
            tbl_stmt += connect_statments(tbl_stmt, True)
            tbl_stmt += "flags = {}".format(desired_flags)

        tbl_stmt += ";"

    return tbl_stmt if "WITH" in tbl_stmt else ""

def generate_alter_statements(current_config, desired_config):
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
            logging.warning(f"""Skipped keyspace '{ks_name}'. Not found in 
                             current config. Add keyspace via DDL schema""")
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
    desired_config = tp.utils.load_config("./configs/excalibur.yaml")

    generate_alter_statements(current_config, desired_config)

if __name__ == "__main__":
    main()