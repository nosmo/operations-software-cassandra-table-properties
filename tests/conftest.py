""" Setup pytest fixtures """
import pytest
import yaml

from src.db import Db, ConnectionParams

# pylint: disable=unused-argument, invalid-name, no-self-use

DB = None


class MockDb():
    """ Mock database class """
    def __init__(self, connection_params: ConnectionParams = None):
        # self._params = connection_params.copy() if connection_params \
        #     else ConnectionParams()
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
        with open("./tests/mocks/excalibur.yaml", "r") as f:
            return yaml.safe_load(f)


@pytest.fixture(scope="session")
def default_database():
    """ Set up local database (localhost)

        Test if a local Cassandra instance is available for testing. If not
        use mock data to run tests.
    """
    global DB  # pylint: disable=global-statement

    DB = Db()

    if DB.check_connection():
        # Delete existing keyspaces
        sel_stmt = "SELECT keyspace_name FROM system_schema.keyspaces"
        rows = DB.exec_query(sel_stmt)

        keyspace_names = [row.get("keyspace_name") for row in rows
                          if not row.get("keyspace_name").startswith("system")]

        for keyspace in keyspace_names:
            DB.exec_query("DROP KEYSPACE IF EXISTS \"{}\"".format(keyspace))

        # Populate instance with sample schemas
        with open("./tests/setup/cql/create_excalibur.cql", "r") as f:
            cmd_stmt = f.read()

        cmds = cmd_stmt.split(";")
        for cmd in cmds:
            trimmed_cmd = cmd.strip()
            if trimmed_cmd:
                DB.exec_query(trimmed_cmd)

        return DB

    return MockDb()


def pytest_sessionfinish():
    """ Tear down after pytest session ends """
    if not DB or isinstance(DB, MockDb):
        return

    # teardown
    if DB.check_connection():
        try:
            with open("./tests/setup/cql/drop_excalibur.cql", "r") as f:
                stmt = f.read()
            DB.exec_query(stmt)
        except Exception:  # pylint: disable=broad-except
            pass
