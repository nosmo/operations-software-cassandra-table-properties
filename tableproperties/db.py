# pylint: disable=invalid-name,too-many-instance-attributes,too-many-arguments
""" Database interface
"""
from abc import abstractmethod, ABC
import configparser
import os
import ssl
from typing import Any, Dict, List, Optional

from cassandra import __version__ as cassver, auth, cluster, query, util, policies

DEFAULT_HOST = "127.0.0.1"
DEFAULT_NATIVE_CQL_PORT = 9042

MAPPED_FIELD_NAMES = {"keyspace_name": "name", "table_name": "name"}


class ConnectionParams:
    """ Cassandra connection parameters """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_NATIVE_CQL_PORT,
        lbp: policies.LoadBalancingPolicy = None,
        username: str = None,
        password: str = None,
        ssl_required: bool = False,
        client_cert_filename: str = None,
        client_key_filename: str = None,
    ):
        """Construct connection settings dictionary.
        Args:
            host:             IP address or hostname
            port:             port number
            lbp:              load balancing policy object
            username:         user name
            password:         password
            ssl_required:     flag whether to use encrypted connection
            client_cert_file: location of client certificate
            client_key_file:  location of client key
        """
        self._host = host if isinstance(host, str) else DEFAULT_HOST
        self._port = port if isinstance(port, int) else DEFAULT_NATIVE_CQL_PORT
        # Default LBP
        self._lbp = lbp
        self._ssl_required = ssl_required  # None = not set
        self._client_cert_filename = client_cert_filename
        self._client_key_filename = client_key_filename
        if ssl_required or (client_cert_filename and client_key_filename):
            self._ssl_context = ssl.SSLContext(
                ssl.PROTOCOL_TLSv1
            )  # type: Optional[ssl.SSLContext]
            # Build options dict for older driver versions
            self._ssl_options = {
                "ssl_version": ssl.PROTOCOL_TLSv1
            }  # type: Optional[Dict[str, Any]]
            if client_cert_filename and client_key_filename:
                self._ssl_context.load_cert_chain(
                    certfile=client_cert_filename, keyfile=client_key_filename
                )
                self._ssl_options["ca_certs"] = client_cert_filename
        else:
            self._ssl_context = None
            self._ssl_options = None

        self._username = username
        self._password = password
        self._auth_provider = None
        self._update_authentication_provider()

    @property
    def host(self) -> str:
        """ Get the host name """
        return self._host if self._host else DEFAULT_HOST

    @host.setter
    def host(self, value: str) -> None:
        """ Set the host name """
        self._host = value if value else DEFAULT_HOST

    @property
    def port(self) -> int:
        """ Get the port number """
        return self._port if self._port else DEFAULT_NATIVE_CQL_PORT

    @port.setter
    def port(self, value: int) -> None:
        """ Set the port """
        self._port = value if value else DEFAULT_NATIVE_CQL_PORT

    @property
    def load_balancing_policy(self) -> policies.LoadBalancingPolicy:
        """ Get the load balancing policy """
        return (
            self._lbp if self._lbp else policies.WhiteListRoundRobinPolicy([self._host])
        )

    @load_balancing_policy.setter
    def load_balancing_policy(self, value: policies.LoadBalancingPolicy):
        """ Set the load balancing policy """
        self._lbp = value

    @property
    def username(self) -> Optional[str]:
        """ Get the username """
        return self._username

    @username.setter
    def username(self, value: str) -> None:
        """ Set the username """
        self._username = value
        self._update_authentication_provider()

    @property
    def password(self) -> Optional[str]:
        """ Get the password """
        return self._password

    @password.setter
    def password(self, value: str) -> None:
        """ Set the password """
        self._password = value
        self._update_authentication_provider()

    @property
    def is_ssl_required(self) -> bool:
        """ Is SSL/TLS required """
        return self._ssl_required if isinstance(self._ssl_required, bool) else False

    @is_ssl_required.setter
    def is_ssl_required(self, value: bool) -> None:
        """ Set SSL/TLS required flag """
        self._ssl_required = value
        self._update_security_context()

    @property
    def client_cert_file(self) -> Optional[str]:
        """ Get the client cert filename """
        return self._client_cert_filename

    @client_cert_file.setter
    def client_cert_file(self, value: str) -> None:
        """ Set the client cert filename """
        self._client_cert_filename = value
        self._update_security_context()

    @property
    def client_key_file(self) -> Optional[str]:
        """ Get the client key filename """
        return self._client_key_filename

    @client_key_file.setter
    def client_key_file(self, value: str) -> None:
        """ Set the client key filename """
        self._client_key_filename = value
        self._update_security_context()

    @property
    def ssl_context(self) -> Optional[ssl.SSLContext]:
        """ SSL Context """
        return self._ssl_context

    @property
    def ssl_options(self) -> Optional[Dict[str, Any]]:
        """ SSL Options """
        return self._ssl_options

    @property
    def auth_provider(self):
        """ Auth Provider """
        return self._auth_provider

    def _update_security_context(self):
        """ Create/recreate the security context """
        if not self._ssl_context:
            self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            self._ssl_options = {"ssl_version": ssl.PROTOCOL_TLSv1}
        if self._client_cert_filename and self._client_key_filename:
            # Only update if both are set
            self._ssl_context.load_cert_chain(
                certfile=self._client_cert_filename, keyfile=self._client_key_filename
            )
            self._ssl_options["ca_certs"] = self._client_cert_filename

    def _update_authentication_provider(self):
        """ Create/recreate the auth provider """
        if self._username and self._password:
            # Only update provider when both are set
            self._auth_provider = auth.PlainTextAuthProvider(
                self._username, self._password
            )
        else:
            self._auth_provider = None

    @staticmethod
    def load_from_rcfile(filename: str):
        """Read and parse a cqlshrc file
        Args:
            filename: location of cqlshrc file

        Returns:
            ConnectionParams object or None in case of failure
        """
        rc_config = configparser.ConfigParser()

        full_rc_filename = os.path.expanduser(filename)
        with open(full_rc_filename, "r") as rc_file:
            rc_config.read_file(rc_file)

        host = rc_config.get("connection", "hostname", fallback=DEFAULT_HOST)
        port = rc_config.getint("connection", "port", fallback=DEFAULT_NATIVE_CQL_PORT)
        use_tls = rc_config.getboolean("connection", "ssl", fallback=False)
        username = rc_config.get("authentication", "username", fallback=None)
        password = rc_config.get("authentication", "password", fallback=None)
        key_file = rc_config.get("ssl", "userkey", fallback=None)
        cert_file = rc_config.get("ssl", "usercert", fallback=None)
        lbp = policies.WhiteListRoundRobinPolicy([host])

        return ConnectionParams(
            host=host,
            port=port,
            username=username,
            password=password,
            ssl_required=use_tls,
            client_cert_filename=cert_file,
            client_key_filename=key_file,
            lbp=lbp,
        )


class AbstractDb(ABC):
    """ Db Interface"""

    @abstractmethod
    def check_connection(self):
        """ Check connection stub """

    @abstractmethod
    def get_current_config(self, drop_ids: bool) -> Optional[Dict[Any, Any]]:
        """ Get current DB config """


class Db(AbstractDb):
    """ Database class """

    def __init__(self, connection_params: ConnectionParams = None):
        self._params = connection_params if connection_params else ConnectionParams()

        self._params.host = (
            self._params.host[0]
            if isinstance(self._params.host, list)
            else self._params.host
        )

        self.cluster = cluster.Cluster(
            [self._params.host],
            load_balancing_policy=self._params.load_balancing_policy,
            port=self._params.port,
            auth_provider=self._params.auth_provider,
        )

        if hasattr(self.cluster, "ssl_context"):
            self.cluster.ssl_context = self._params.ssl_context
        else:
            # driver versions < 3.17.0 do not have support for ssl_context
            self.cluster.ssl_options = self._params.ssl_options

    #TODO replace this function with ast.literal_eval
    @staticmethod
    def convert_value(val: Any) -> Any:
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
    def convert_mapped_props(subconfig) -> dict:
        """Convert mapped properties

        Args:
            subconfig: Mapping properties

        Returns:
            Dictionary with converted object properties
        """
        #TODO replace with ast.literal_eval
        return (
            {key: Db.convert_value(val) for key, val in subconfig.items()}
            if isinstance(subconfig, util.OrderedMapSerializedKey)
            else {}
        )

    def exec_query(self, query_stmt: str) -> list:
        """Execute Cassandra query

        Args:
            query_stmt: CQL query

        Returns:
            List of rows or empty list
        """
        with self.cluster.connect() as session:
            session.row_factory = query.ordered_dict_factory
            rows = session.execute(query_stmt)

            return rows.current_rows if hasattr(rows, "current_rows") else []

    def check_connection(self) -> bool:
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

                if isinstance(val, util.OrderedMapSerializedKey):
                    val = Db.convert_mapped_props(val)
                elif key == "flags":
                    val = list(val) if isinstance(val, util.SortedSet) else val
                keyspace[mapped_key] = val
            keyspace_configs.append(keyspace)

        return {"keyspaces": keyspace_configs}

    def get_table_configs(
        self, keyspace_name: str, drop_ids: bool
        ) -> List[Dict[str, Any]]:
        """Retrieve table properties

        Args:
            keyspace_name: Keyspace name

        Returns:
            Table properties in dictionary
        """
        table_configs = []

        query_stmt = (
            "SELECT * FROM system_schema.tables "
            "WHERE keyspace_name = '{}';".format(keyspace_name)
        )

        rows = self.exec_query(query_stmt)
        for row in rows:
            tbl = {}
            for key, val in row.items():
                if key == "keyspace_name" or (key == "id" and drop_ids):
                    continue
                elif isinstance(val, util.OrderedMapSerializedKey):
                    val = Db.convert_mapped_props(val)
                elif key == "flags":
                    val = list(val) if isinstance(val, util.SortedSet) else val
                elif key == "id":
                    val = str(val)
                tbl[MAPPED_FIELD_NAMES.get(key, key)] = val

            table_configs.append(tbl)

        return table_configs

    def get_current_config(self, drop_ids: bool = False) -> Optional[Dict[Any, Any]]:
        """Retrieve the current config from the Cassandra instance.

        Args:
            connection: Connection parameters

        Returns:
            Dictionary with keyspace and table properties or None
        """
        keyspace_config = self.get_keyspace_configs()
        keyspaces = keyspace_config.get("keyspaces")
        if not keyspaces:
            return None

        for keyspace in keyspace_config.get("keyspaces", []):
            keyspace["tables"] = self.get_table_configs(keyspace.get("name"), drop_ids)

        return keyspace_config
