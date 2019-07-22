# pylint: disable=invalid-name,too-many-instance-attributes,too-many-arguments
""" Database interface
"""
import configparser
import logging
import os
import ssl

import cassandra
import cassandra.auth
import cassandra.cluster
import cassandra.query
import cassandra.util
import cassandra.policies

DEFAULT_HOST = '127.0.0.1'
DEFAULT_NATIVE_CQL_PORT = 9042

MAPPED_FIELD_NAMES = {"keyspace_name": "name", "table_name": "name"}


class ConnectionParams():
    """ Cassandra connection parameters """

    def __init__(self,
                 host: str = None,
                 port: int = None,
                 lbp: cassandra.policies.LoadBalancingPolicy = None,
                 username: str = None,
                 password: str = None,
                 ssl_required: bool = False,
                 client_cert_filename: str = None,
                 client_key_filename: str = None):
        """Construct connection settings dictionary.
        Args:
            host:             IP address or hostname
            port:             port number
            lbp:              load balancing policy object
            username:         user name
            password:         password
            ssl_required:          flag whether to use encrypted connection
            client_cert_file: location of client certificate
            client_key_file:  location of client key
        Returns:
            Connection settings dictionary.
        """
        self._host = [host] if isinstance(host, str) else [DEFAULT_HOST]
        self._port = port if port and isinstance(port, int) \
            else DEFAULT_NATIVE_CQL_PORT
        # Default LBP
        self._lbp = lbp if lbp else \
            cassandra.policies.WhiteListRoundRobinPolicy(self._host)
        self._ssl_required = ssl_required   # None = not set
        self._client_cert_filename = client_cert_filename
        self._client_key_filename = client_key_filename
        if ssl_required or (client_cert_filename and client_key_filename):
            self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            if client_cert_filename and client_key_filename:
                self._ssl_context.load_cert_chain(
                    certfile=client_cert_filename, keyfile=client_key_filename)
        else:
            self._ssl_context = None

        self._username = username
        self._password = password
        self._auth_provider = None
        self.update_authentication_provider()

    @property
    def host(self):
        """ Get the host name """
        return self._host if self._host else DEFAULT_HOST

    @host.setter
    def host(self, value):
        """ Set the host name """
        self._host = value if value else DEFAULT_HOST

    @property
    def port(self):
        """ Get the port number """
        return self._port if self._port else DEFAULT_NATIVE_CQL_PORT

    @port.setter
    def port(self, value):
        """ Set the port """
        self._port = value if value else DEFAULT_NATIVE_CQL_PORT

    @property
    def load_balancing_policy(self):
        """ Get the load balancing policy """
        return self._lbp

    @load_balancing_policy.setter
    def load_balancing_policy(self, value):
        """ Set the load balancing policy """
        self._host = value

    @property
    def username(self):
        """ Get the username """
        return self._username

    @username.setter
    def username(self, value):
        """ Set the username """
        self._username = value
        self.update_authentication_provider()

    @property
    def password(self):
        """ Get the password """
        return self._password

    @password.setter
    def password(self, value):
        """ Set the password """
        self._password = value
        self.update_authentication_provider()

    @property
    def is_ssl_required(self):
        """ Is SSL/TLS required """
        return self._ssl_required if isinstance(self._ssl_required, bool) \
            else False

    @is_ssl_required.setter
    def is_ssl_required(self, value):
        """ Set SSL/TLS required flag """
        self._ssl_required = value
        self.update_security_context()

    @property
    def client_cert_file(self):
        """ Get the client cert filename """
        return self._client_cert_filename

    @client_cert_file.setter
    def set_client_cert_file(self, value):
        """ Set the client cert filename """
        self._client_cert_filename = value
        self.update_security_context()

    @property
    def client_key_file(self):
        """ Get the client key filename """
        return self._client_key_filename

    @client_key_file.setter
    def set_client_key_file(self, value):
        """ Set the client key filename """
        self._client_key_filename = value
        self.update_security_context()

    @property
    def ssl_context(self):
        """ SSL Context """
        return self._ssl_context

    @property
    def auth_provider(self):
        """ Auth Provider """
        return self._auth_provider

    def update_security_context(self):
        """ Create/recreate the security context """
        if not self._ssl_context:
            self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        if self._client_cert_filename and self._client_key_filename:
            # Only update if both are set
            self._ssl_context.load_cert_chain(
                certfile=self._client_cert_filename,
                keyfile=self._client_key_filename)

    def update_authentication_provider(self):
        """ Create/recreate the auth provider """
        if self._username and self._password:
            # Only update provider when both are set
            self._auth_provider = \
                cassandra.auth.PlainTextAuthProvider(self._username,
                                                     self._password)
        else:
            self._auth_provider = None

    @staticmethod
    def load_from_rcfile(filename: str):
        """ Read and parse a cqlshrc file
        Args:
            filename: location of cqlshrc file

        Returns:
            ConnectionParams object or None in case of failure
        """
        rc_config = configparser.ConfigParser()

        try:
            full_rc_filename = os.path.expanduser(filename)
            with open(full_rc_filename, "r") as rc_file:
                rc_config.read_file(rc_file)
        except Exception as ex:  # pylint: disable=broad-except
            logging.exception(ex)
            raise Exception("File '{}' not found or could not be read.".format(
                filename
            ))

        host = rc_config.get("connection", "hostname", fallback=None)
        port = rc_config.getint("connection", "port",
                                fallback=DEFAULT_NATIVE_CQL_PORT)
        use_tls = rc_config.getboolean("connection", "ssl",
                                       fallback=False)
        username = rc_config.get("authentication", "username", fallback=None)
        password = rc_config.get("authentication", "password", fallback=None)
        key_file = rc_config.get("ssl", "userkey", fallback=None)
        cert_file = rc_config.get("ssl", "usercert", fallback=None)

        return ConnectionParams(host=host, port=port, username=username,
                                password=password, ssl_required=use_tls,
                                client_cert_filename=cert_file,
                                client_key_filename=key_file)


class Db():
    """ Database class """
    def __init__(self, connection_params: ConnectionParams = None):
        self._params = connection_params if connection_params \
            else ConnectionParams()

        self.cluster = cassandra.cluster.Cluster(
            self._params.host,
            load_balancing_policy=self._params.load_balancing_policy,
            port=self._params.port,
            auth_provider=self._params.auth_provider,
            ssl_context=self._params.ssl_context)

    @staticmethod
    def convert_value(val: any) -> any:
        """Convert a string to correct int or float where possible

        Args:
            val: Value expression to be converted

        Returns:
            Either the original value or an integer or float
        """
        if not isinstance(val, str):
            return val

        try:
            num_int = int(val)
            return num_int
        except ValueError:
            pass

        try:
            num_float = float(val)
            return num_float
        except ValueError:
            pass

        return val

    @staticmethod
    def convert_mapped_props(subconfig):
        """Convert mapped properties

        Args:
            subconfig: Mapping properties

        Returns:
            Dictionary with converted object properties
        """
        if not isinstance(subconfig, cassandra.util.OrderedMapSerializedKey):
            return {}

        d = {key: Db.convert_value(val) for key, val in subconfig.items()}

        return d

    def exec_query(self, query_stmt: str) -> list:
        """Execute Cassandra query

        Args:
            query_stmt: CQL query

        Returns:
            List of rows or empty list
        """
        try:
            with self.cluster.connect() as session:
                session.row_factory = cassandra.query.ordered_dict_factory
                rows = session.execute(query_stmt)

                return rows.current_rows if hasattr(rows,
                                                    "current_rows") else []
        except (cassandra.cluster.NoHostAvailable) as ex:
            if hasattr(ex, "errors") and ex.errors:
                for err, obj in ex.errors.items():
                    msg = "Host {} responded with '{}'".format(err, str(obj))
                    if isinstance(obj, cassandra.cluster.ConnectionShutdown):
                        msg += " " if msg[-1] == "." else ". "
                        msg += "Is SSL/TLS required to connect to the host?"
                    print(msg)
            else:
                logging.exception(ex)
            raise Exception("Connection failed")

        except Exception as ex:
            logging.exception(ex)
            raise

        return []

    def check_connection(self):
        """Test Cassandra connectivity

        Returns:
            True if connection was successful. False otherwise
        """
        return self.exec_query("SELECT cql_version FROM system.local;") != []

    def get_keyspace_configs(self) -> dict:
        """Retrieve all keyspace properties.

        Returns:
            Dictionary with keyspace settings.
        """
        keyspace_configs = []

        rows = self.exec_query("SELECT * FROM system_schema.keyspaces;")
        for row in rows:
            # Skip system tables.
            if row.get("keyspace_name").startswith("system"):
                continue

            keyspace = {}
            for key, val in row.items():
                mapped_key = MAPPED_FIELD_NAMES.get(key, key)

                if isinstance(val, cassandra.util.OrderedMapSerializedKey):
                    val = Db.convert_mapped_props(val)
                elif key == "flags":
                    val = list(val) if \
                        isinstance(val, cassandra.util.SortedSet) else val
                keyspace[mapped_key] = val
            keyspace_configs.append(keyspace)

        return {"keyspaces": keyspace_configs}

    def get_table_configs(self, keyspace_name: str, drop_ids: bool) -> dict:
        """Retrieve table properties

        Args:
            keyspace_name: Keyspace name

        Returns:
            Table properties in dictionary
        """
        table_configs = []

        query_stmt = "SELECT * FROM system_schema.tables " \
                     "WHERE keyspace_name = '{}';".format(keyspace_name)

        rows = self.exec_query(query_stmt)
        for row in rows:
            tbl = {}
            for key, val in row.items():
                if key == "keyspace_name" or (key == "id" and drop_ids):
                    continue
                elif isinstance(val, cassandra.util.OrderedMapSerializedKey):
                    val = Db.convert_mapped_props(val)
                elif key == "flags":
                    val = list(val) \
                        if isinstance(val, cassandra.util.SortedSet) else val
                elif key == "id":
                    val = str(val)
                tbl[MAPPED_FIELD_NAMES.get(key, key)] = val

            table_configs.append(tbl)

        return table_configs

    def get_current_config(self, drop_ids: bool = False) -> dict:
        """Retrieve the current config from the Cassandra instance.

        Args:
            connection: Connection parameters

        Returns:
            Dictionary with keyspace and table properties.
        """
        keyspaces = self.get_keyspace_configs()

        for keyspace in keyspaces.get("keyspaces", []):
            keyspace["tables"] = self.get_table_configs(
                keyspace.get("name"), drop_ids)

        return keyspaces
