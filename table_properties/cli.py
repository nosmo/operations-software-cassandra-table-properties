# pylint: disable=broad-except, invalid-name
""" CLI interface class
"""
import argparse
import getpass
import logging
import os
import sys

import table_properties as tp


class TablePropertiesCli():
    """Command-line interface class
    """

    def __init__(self):
        self._args = None

    @staticmethod
    def get_arg_parser()->argparse.ArgumentParser:
        """Set up CLI arguments in parser

        Returns:
            argparse.ArgumentParser
        """
        msg = "Compare actual Cassandra keyspace and table properties to " \
              "desired properties defined in a YAML file and create ALTER " \
              "KEYSPACE and ALTER TABLE statements for properties that " \
              "differ."
        parser = argparse.ArgumentParser(prog=tp.PROG_NAME,
                                         description=msg,
                                         formatter_class=lambda prog:
                                         argparse.RawTextHelpFormatter(
                                             prog, width=120))

        parser.add_argument(metavar="<filename>",
                            nargs="?",
                            dest="config_filename",
                            help="Desired configuration YAML file")

        parser.add_argument("-c",
                            "--contactpoint",
                            metavar="<ip>",
                            dest="host_ip",
                            help="Host IP address or name. Default: localhost",
                            required=False)

        parser.add_argument("-C",
                            "--clientcert",
                            metavar="<filename>",
                            dest="client_cert_file",
                            help="Client cert file name.",
                            required=False)

        parser.add_argument("-d",
                            "--dump",
                            dest="dump_config",
                            help="Dump current configuration to STDOUT",
                            action="store_true")

        parser.add_argument("-k",
                            "--clientkey",
                            metavar="<filename>",
                            dest="client_key_file",
                            help="Client key file name.",
                            required=False)

        parser.add_argument("-l",
                            "--log",
                            metavar="<filename>",
                            dest="log_file",
                            help="Log file name. If none is provied, "
                                 "STDERR is used.",
                            required=False)

        parser.add_argument("-p",
                            "--port",
                            type=int,
                            metavar="<port #>",
                            dest="host_port",
                            help="Port number. Default: 9042",
                            required=False)

        parser.add_argument("-P",
                            "--password",
                            dest="password_reqd",
                            help="Prompt for password.",
                            action="store_true")

        parser.add_argument("-r",
                            "--rcfile",
                            metavar="<filename>",
                            dest="rc_file",
                            help="cqlrc file name. "
                                 "Default: ~/.cassandra/cqlshrc",
                            required=False)

        parser.add_argument("-t", "--tls",
                            dest="use_ssl",
                            help="Use TLS encryption for client server "
                                 "communication.",
                            action="store_true")

        parser.add_argument("-u",
                            "--username",
                            metavar="<user name>",
                            dest="username",
                            help="User name for plain text authentication.",
                            required=False)

        parser.add_argument("-v",
                            "--version",
                            action="version",
                            version="{} {}".format(tp.PROG_NAME,
                                                   tp.PROG_VERSION))

        return parser

    def execute(self, args: list) -> None:
        """Execute applicaton
        """
        password = None
        log_level = os.environ.get("TP_LOG_LEVEL", tp.utils.DEFAULT_LOG_LEVEL)

        parser = TablePropertiesCli.get_arg_parser()

        # Load the desired configuration from file
        self._args = parser.parse_args(args=args)

        # Modify root logger settings
        tp.utils.setup_logging(self._args.log_file,
                               tp.utils.get_log_level(log_level))

        # Get password from user if required
        if self._args.password_reqd:
            password = getpass.getpass(prompt="Password: ")

        config_filename = None
        conn_params = tp.database.ConnectionParams(
            host=self._args.host_ip,
            port=self._args.host_port,
            username=self._args.username,
            password=password,
            ssl_required=self._args.use_ssl,
            client_cert_filename=self._args.client_cert_file,
            client_key_filename=self._args.client_key_file)

        try:
            if self._args.dump_config or self._args.config_filename:
                if self._args.rc_file:
                    if not os.path.exists(self._args.rc_file):
                        print("File '{}' not found.")
                        sys.exit(1)
                    conn_params = \
                        tp.database.ConnectionParams.load_from_rcfile(
                            self._args.rc_file)

                # Construct the connection parameters
                db = tp.database.Db(conn_params)

                # Read current configuration from database
                current_config = db.get_current_config()

                if self._args.dump_config:
                    try:
                        print(tp.utils.format_yaml(current_config))
                    except Exception as ex:
                        logging.exception(ex)
                else:
                    config_filename = self._args.config_filename
                    logging.info("Reading config from '%s'", config_filename)
                    desired_config = tp.utils.load_yaml(config_filename)

                    # Generate ALTER statements for Keyspaces and Tables
                    alter_statements = tp.generator. \
                        generate_alter_statements(current_config,
                                                  desired_config)

                    print(alter_statements)
            else:
                self.get_arg_parser().print_usage()
        except Exception as ex:
            logging.exception(ex)
            print(ex)


def main():
    """ Main function
    """
    cmd = TablePropertiesCli()
    cmd.execute(args=sys.argv[1:])

if __name__ == "__main__":
    main()
