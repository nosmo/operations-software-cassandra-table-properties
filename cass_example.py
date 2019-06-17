from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import collections
import json

cluster = Cluster()

session = cluster.connect("system_schema")
session.row_factory = dict_factory

current_config = { "keyspaces": [] }

rows = session.execute("SELECT * FROM keyspaces")
for row in rows.current_rows:
    # ks = collections.namedtuple('Keyspace', row.keys())
    # print(ks.__dict__)
    for k,v in row.items():
        print(f"Key {k} : {v}")


# rows = session.execute("SELECT * FROM tables")
# for row in rows:
#     print(row)
