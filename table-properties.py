#!/usr/bin/env python3
import sys

ver_major = sys.version_info[0]
ver_minor = sys.version_info[1]

if ver_major < 3 or (ver_major == 3 and ver_minor < 4):
    print("This app requires Python 3.4 or higher. Version used : {}.{}"
        .format(ver_major,ver_minor))
    exit(1)

from table_properties import cli

cli.main()