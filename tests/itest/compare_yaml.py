#!/usr/bin/env python3

import pprint
import sys

import dictdiffer
import yaml

def main():

    if len(sys.argv) != 3:
        sys.stderr.write("Usage: compare_yaml.py file.yaml file2.yaml")
        sys.exit(2)
    yaml1 = yaml.load(open(sys.argv[1]), Loader=yaml.SafeLoader)
    yaml2 = yaml.load(open(sys.argv[2]), Loader=yaml.SafeLoader)

    # delete table IDs for comparison purposes - they'll be randomly generated
    # on creation. -I flag on script removes this requirement
    for ks_id, keyspace in enumerate(yaml1["keyspaces"]):
        for tb_id, table in enumerate(keyspace["tables"]):
            if "id" in yaml1["keyspaces"][ks_id]["tables"][tb_id]:
                del(yaml1["keyspaces"][ks_id]["tables"][tb_id]["id"])
            if "id" in yaml2["keyspaces"][ks_id]["tables"][tb_id]:
                del(yaml2["keyspaces"][ks_id]["tables"][tb_id]["id"])

    retcode = 0

    if yaml1 != yaml2:
        print("YAML files differ!")
        print("Diff:")
        pprint.pprint(list(dictdiffer.diff(yaml1, yaml2)))
        print("File 1 contains: ")
        pprint.pprint(yaml1)
        print("File 2 contains: ")
        pprint.pprint(yaml2)
        retcode = 1
    else:
        print("Compared YAML files match!")

    sys.exit(retcode)

if __name__ == "__main__":
    main()
