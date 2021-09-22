#!/usr/bin/env python3
"""Main entry point
"""
# pylint: disable=wrong-import-position, invalid-name
import sys

PY_VER_MAJOR = sys.version_info[0]
PY_VER_MINOR = sys.version_info[1]

if PY_VER_MAJOR < 3 or (PY_VER_MAJOR == 3 and PY_VER_MINOR < 4):
    print("This app requires Python 3.4 or higher. Version used : {}.{}"
          .format(PY_VER_MAJOR, PY_VER_MINOR))
    exit(1)

from tableproperties import cli  # noqa
cli.main()
