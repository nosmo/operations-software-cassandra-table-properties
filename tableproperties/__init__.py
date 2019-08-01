""" Cassandra table properties package """

# pylint: disable=invalid-name
import sys

from pkg_resources import DistributionNotFound, get_distribution

PY_VER_MAJOR = sys.version_info[0]
PY_VER_MINOR = sys.version_info[1]

if PY_VER_MAJOR < 3 or (PY_VER_MAJOR == 3 and PY_VER_MINOR < 4):
    print("This app requires Python 3.4 or higher. Version used : {}.{}"
          .format(PY_VER_MAJOR, PY_VER_MINOR))
    exit(1)

try:
    from . import db, utils, generator
except ImportError as iex:
    print(iex)
    print("Please run 'pip install -r requirements.txt'")
    exit(1)

PROG_NAME = "cassandra-table-properties"

try:
    # Must be the same used as 'name' in setup.py
    __version__ = get_distribution(PROG_NAME).version
except DistributionNotFound:
    pass  # package is not installed
