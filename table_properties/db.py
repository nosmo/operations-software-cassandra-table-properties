from cassandra import cluster as cc, query as cq, util

DEFAULT_PORT = 9042

def get_default_connection():
    return get_local_connection()

def get_local_connection():
    return { "hosts": ["localhost"], "port": DEFAULT_PORT }

def get_class_name(full_class_name: str):
    return full_class_name.split(".")[-1]

def convert_value(val):
    if not isinstance(val, str):
        return val

    if val.isnumeric():
        return int(val)
    elif "." in val and val.replace(".").isdigit():
        return float(val)

    return val

def key_mapper(orig_key):
    mapping = {
        "keyspace_name": "name",
        "table_name": "name"
    }

    return mapping.get(orig_key, orig_key)

def get_replication_settings(replication):
    if not isinstance(replication, util.OrderedMapSerializedKey):
        return {}

    # repl_config = dict(replication.items())
    repl_config = dict()
    data_centers = list()
    for k in replication.keys():
        if k == "class":
            repl_config["class"] = get_class_name(replication["class"])
        elif k == "replication_factor":
            repl_config["replication_factor"] = \
                int(replication["replication_factor"])
        else:
            # data center name
            data_centers.append({
                "name": k,
                "replication_factor": int(replication[k])
            })
    repl_config["data_centers"] = data_centers

    return repl_config

def get_subconfig_settings(subconfig):
    if not isinstance(subconfig, util.OrderedMapSerializedKey):
        return {}

    settings = dict()
    for k, v in subconfig.items():
        if k == "class":
            v = get_class_name(v)

        settings[k] = convert_value(v)

    return settings
    
def get_compression_settings(compression):
    return get_subconfig_settings(compression)

def get_extension_settings(extension):
    return get_subconfig_settings(extension)

def get_compaction_settings(compaction):
    return get_subconfig_settings(compaction)

def get_caching_settings(caching):
    return get_subconfig_settings(caching)

def get_keyspace_configs(connection: dict):
    keyspace_configs = []

    cluster = cc.Cluster(
        connection.get("hosts"),
        port=connection.get("port")
    )

    with cluster.connect("system_schema") as session:
        session.row_factory = cq.ordered_dict_factory

        rows = session.execute("SELECT * FROM keyspaces")
        for row in rows.current_rows:
            ks_name = row.get("keyspace_name").lower()
            if ks_name.startswith("system"):
                continue

            ks = {}
            for k,v in row.items():
                nk = key_mapper(k)
                if nk == "replication":
                    v = get_replication_settings(v)
                ks[nk] = v
            keyspace_configs.append(ks)

    return { "keyspaces": keyspace_configs }

def get_table_configs(connection: dict, keyspace_name: str):
    table_configs = []

    cluster = cc.Cluster(
        connection.get("hosts", get_local_connection()["hosts"]),
        port=connection.get("port", get_local_connection()["port"]))

    with cluster.connect("system_schema") as session:
        session.row_factory = cq.ordered_dict_factory

        sql_stmt = f"SELECT * FROM tables WHERE keyspace_name = \
                     '{keyspace_name}';"

        rows = session.execute(sql_stmt)
        for row in rows.current_rows:
            t = {}
            for k,v in row.items():
                if k == "keyspace_name":
                    continue
                elif k == 'caching':
                    v = get_caching_settings(v)
                elif k == "compression":
                    v = get_compression_settings(v)
                elif k == "compaction":
                    v = get_compaction_settings(v)
                elif k == "extensions":
                    v = get_extension_settings(v)
                elif k == "flags":
                    v = list(v) if isinstance(v, util.SortedSet) else v
                elif k == "id":
                    v = str(v)
                t[key_mapper(k)] = v

            table_configs.append(t)

    return table_configs

def get_current_config(connection: dict = None):
    if not connection:
        connection = get_local_connection()

    keyspaces = get_keyspace_configs(connection)
    
    for ks in keyspaces.get("keyspaces", []):
        ks["tables"] = get_table_configs(connection, ks.get("name"))

    return keyspaces


def main():
    local_connection = get_local_connection()
    for ks in get_current_config(local_connection).get("keyspaces", []):
        print("Keyspace : {}".format(ks.get("name", "")))
        print("- durable_writes : {}".format(ks.get("name", "")))
        print("- Tables")
        for t in ks.get("tables", []):
            print("\t\t- {}".format(t.get("name")))

if __name__ == "__main__":
    main()
