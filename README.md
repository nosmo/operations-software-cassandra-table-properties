# cassandra-table-properties

[![Build Status](https://travis-ci.com/hknustwmf/cassandra-table-properties.svg?branch=master)](https://travis-ci.com/hknustwmf/cassandra-table-properties)
[![codecov](https://codecov.io/gh/hknustwmf/cassandra-table-properties/branch/master/graph/badge.svg)](https://codecov.io/gh/hknustwmf/cassandra-table-properties)

Cassandra table and keyspace configuration tool.

## Installation

Python 3.4 or higher is required. Install the dependencies with

```bash
pip install -r requirements.txt
```

## Usage

From the command prompt, execute below to get help on available options

```bash
table-properties -h
```

To dump the current configuration into YAML use

```bash
table-properties -d <filename> [-f]
```

You may get a warning if the file already exists. Add `-f` to force overwrite.

Once you have an existing configuration, you can make changes to that YAML file and use it as an input file.

```bash
table-properties <filename>
```

This generates ALTER statements if any differences between the configurations were detected. Use

```bash
table-properties <filename> | cqlsh
```

to update Cassandra's configuration.

### Changing Defaults and Using `cqlshrc`

If the server connection is different from the default values, a `cqlrcsh` file can be used to provide those settings. The default file location is in `$HOME/.cassandra/cqlrcsh` but can be changed with the `-r` switch. Use `-s` to not read `cqlrcsh`.

```bash
table-properties -r <filename>
```

#### Other switches

```shell
  -c <ip 1>[,...,<ip n>], --contactpoint <ip 1>[,...,<ip n>]
                        Host IP address(es) or name(s).Default: localhost

  -C <filename>, --clientcert <filename>
                        Client cert file name.

  -k <filename>, --clientkey <filename>
                        Client key file name.

  -l <filename>, --log <filename>
                        Log file name. Default: tp_YYYYMMDD-HHMMSS.log

  -p <port number>, --port <port number>
                        Port number. Default: 9042

  -o <protocol version>, --protocolversion <protocol version>
                        Cassandra driver protocol version (1-5).Default: 2

  -P <password>, --password <password>
                        Password for plain text authentication.

  -t, --tls             Use TLS encryption for client server communication.

  -u <user name>, --username <user name>
                        User name for plain text authentication.

  -v, --version         show program's version number and exit
```
