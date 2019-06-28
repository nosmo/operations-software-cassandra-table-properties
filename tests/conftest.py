import yaml
import subprocess

import pytest

import table_properties.db as db

@pytest.fixture(scope="session")
def current_config():
    """ Injects current config data into tests
        
        Test if a local Cassandra instance is available for testing. If not
        use mock data to run tests.
    """
    conn_params = db.get_local_connection()
    if db.check_connection(conn_params):
        subprocess.call("./tests/clean_db.sh")
        subprocess.call("./tests/setup_db.sh")

        return db.get_current_config(conn_params, True)
    else:
        # Load the mock config
        with open("./tests/mocks/excalibur.yaml", "r") as f:
            return yaml.safe_load(f)


def pytest_sessionfinish(session, exitstatus):
    conn_params = db.get_local_connection()
    # teardown
    if db.check_connection(conn_params):
        subprocess.call("./tests/clean_db.sh")
