""" Setup pytest fixtures """
import pytest
import yaml

from tableproperties.db import AbstractDb, ConnectionParams

# pylint: disable=invalid-name


class MockDb(AbstractDb):
    """ Mock database class """
    def __init__(self, connection_params: ConnectionParams = None):
        pass

    def check_connection(self):
        """Test Cassandra connectivity

        Returns:
            True
        """
        return True

    def get_current_config(self, drop_ids: bool = False) -> dict:
        """Retrieve the current config from the Cassandra instance.

        Args:
            connection: Connection parameters

        Returns:
            Dictionary with keyspace and table properties.
        """
        # Load the mock config
        with open("./tableproperties/tests/mocks/excalibur.yaml", "r") as f:
            return yaml.safe_load(f)


@pytest.fixture()
def default_database():
    """ Set up local database (localhost)

        Returns mock configuration data to run tests.
    """
    return MockDb()
