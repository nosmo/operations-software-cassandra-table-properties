#pylint: disable = c-extension-no-member, fixme
""" Database interface
"""
import logging

import cassandra.cluster
import cassandra.query
import cassandra.util
import cassandra.policies

DEFAULT_PORT = 9042

def get_default_connection() -> dict:
    """Get default connection parameters.

    Returns:
        Connection settings dictionary.
    """
    # TODO: Do we need a config or CLI arguments for this? What about LBP?
    return get_local_connection()

def get_local_connection() -> dict:
    """Return local connection parameters.

    Returns:
        Local connection settings dictionary.
    """
    return {
        "hosts": ["localhost"],
        "port": DEFAULT_PORT,
        "load_balancing_policy" : cassandra.policies.RoundRobinPolicy()
    }

def get_class_name(full_class_name: str) -> str:
    """Return actual class name

    Args:
        full_class_name: Full Java class including namespace

    Examples:
        >>> cls="org.apache.cassandra.locator.NetworkTopologyStrategy"
        >>> print(table_properties.db.get_class_name(cls))
        NetworkTopologyStrategy

    Returns:
        Actual class name
    """
    return full_class_name.split(".")[-1]

def convert_value(val: any) -> any:
    """Convert a string to a number

    Args:
        val: Value expression to be converted

    Returns:
        Either the original value or an integer or float
    """
    if not isinstance(val, str):
        return val

    try:
        num_int = int(val)
        return num_int
    except ValueError:
        pass

    try:
        num_float = float(val)
        return num_float
    except ValueError:
        pass

    return val

def key_mapper(orig_key) -> str:
    """ Return the id field name

    Args:
        orig_key: Original key name

    Returns:
        The mapped key name or key name itself
    """
    mapping = {"keyspace_name": "name", "table_name": "name"}

    return mapping.get(orig_key, orig_key)

def get_replication_settings(replication: dict) -> dict:
    """Return mapping with replication settings

    Args:
        replication: Mapping properties in dictionary

    Returns:
        Dictionary with converted replication properties.
    """
    if not isinstance(replication, cassandra.util.OrderedMapSerializedKey):
        return {}

    repl_config = dict()
    data_centers = list()
    for key in replication.keys():
        if key == "class":
            repl_config["class"] = get_class_name(replication["class"])
        elif key == "replication_factor":
            repl_config["replication_factor"] = \
                convert_value(replication["replication_factor"])
        else:
            # data center name
            data_centers.append({
                "name": key,
                "replication_factor": convert_value(replication[key])
            })
    repl_config["data_centers"] = data_centers

    return repl_config

def get_subconfig_settings(subconfig):
    """Convert mapped properties

    Args:
        subconfig: Mapping properties

    Returns:
        Dictionary with converted object properties
    """
    if not isinstance(subconfig, cassandra.util.OrderedMapSerializedKey):
        return {}

    settings = dict()
    for key, value in subconfig.items():
        if key == "class":
            value = get_class_name(value)

        settings[key] = convert_value(value)

    return settings

def exec_query(connection: dict, query_stmt: str) -> list:
    """Execute Cassandra query

    Args:
        connection: connection paramters
        query_stmt: CQL query

    Returns:
        List of rows or empty list
    """
    try:
        cluster = cassandra.cluster.Cluster(
            connection.get("hosts", get_local_connection()["hosts"]),
            port=connection.get("port", get_local_connection()["port"]),
            load_balancing_policy=connection.get("load_balancing_policy", \
                get_local_connection()["load_balancing_policy"])
        )

        with cluster.connect("system_schema", wait_for_all_pools=True) as session:
            session.row_factory = cassandra.query.ordered_dict_factory
            rows = session.execute(query_stmt)

            return rows.current_rows
    except cassandra.cluster.NoHostAvailable as nhex:
        logging.exception(nhex)
        raise Exception("Failed to connect to Cassandra. See log for details.")
    except Exception as ex:
        logging.exception(ex)
        raise

    return []

def get_keyspace_configs(connection: dict) -> dict:
    """Retrieve all keyspace properties.

    Args:
        connection: Connection parameters

    Returns:
        Dictionary with keyspace settings.
    """
    keyspace_configs = []

    rows = exec_query(connection, "SELECT * FROM keyspaces")
    for row in rows:
        ks_name = row.get("keyspace_name").lower()
        if ks_name == "system" or ks_name.startswith("system_"):
            continue

        keyspace = {}
        for key, val in row.items():
            mapped_key = key_mapper(key)
            if mapped_key == "replication":
                val = get_replication_settings(val)
            keyspace[mapped_key] = val
        keyspace_configs.append(keyspace)

    return {"keyspaces": keyspace_configs}

def get_table_configs(connection: dict, keyspace_name: str) -> dict:
    """Retrieve table properties

    Args:
        connection:    Connection parameters in dictionary
        keyspace_name: Keyspace name

    Returns:
        Table properties in dictionary
    """
    table_configs = []

    query_stmt = "SELECT * FROM tables WHERE keyspace_name = '{}';" \
        .format(keyspace_name)

    rows = exec_query(connection, query_stmt)
    for row in rows:
        tbl = {}
        for key, val in row.items():
            if key == "keyspace_name":
                continue
            elif isinstance(val, cassandra.util.OrderedMapSerializedKey):
                if key == "replication":
                    val = get_replication_settings(val)
                else:
                    val = get_subconfig_settings(val)
            elif key == "flags":
                val = list(val) if isinstance(val, cassandra.util.SortedSet) else val
            elif key == "id":
                val = str(val)
            tbl[key_mapper(key)] = val

        table_configs.append(tbl)

    return table_configs

def get_current_config(connection: dict = None) -> dict:
    """Retrieve the current config from the Cassandra instance.

    Args:
        connection: Connection parameters

    Returns:
        Dictionary with keyspace and table properties.
    """
    if not connection:
        connection = get_local_connection()

    keyspaces = get_keyspace_configs(connection)

    for keyspace in keyspaces.get("keyspaces", []):
        keyspace["tables"] = get_table_configs(connection, keyspace.get("name"))

    return keyspaces


def main():
    """ Main function
    """
    local_connection = get_local_connection()
    for keyspace in get_current_config(local_connection).get("keyspaces", []):
        print("\nKeyspace : {}".format(keyspace.get("name", "")))
        print("- durable_writes : {}".format(keyspace.get("name", "")))
        print("- Tables")
        for tbl in keyspace.get("tables", []):
            print("\t\t- {}".format(tbl.get("name")))

if __name__ == "__main__":
    main()
