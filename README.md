# cassandra-table-properties

Cassandra table and keyspace configuration tool.

# Installation

Python 3.4 or higher is required. Install the dependencies with

`pip install -f requirements.txt`

# Usage:

From the command prompt, execute below to get help on available options

`table-properties -h`

To dump the current configuration into YAML use

`table-properties -o <filename> [-f]`

You may get a warning if the file already exists. Add `-f` to force overwrite.

Once you have an existing configuration, you can make changes to that YAML file and use it as an input file.

`table-properties <filename>`

This generates ALTER statements if any differences between the configurations were detected. Use

`table-properties <filename> | cqlsh`

to update the configuration of Cassandra.
