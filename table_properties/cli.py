# pylint: disable=broad-except, invalid-name
""" CLI interface class
"""
import argparse
import getpass
import logging
import os
import sys

import yaml

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

        parser.add_argument("-i",
                            "--ip",
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

        parser.add_argument("-q", "--quiet",
                            dest="run_quiet",
                            help="When the flag is set exit with 0 only if the "
                                 "configuration matches the YAML file. Exit "
                                 "with 1 otherwise.",
                            action="store_true")

        parser.add_argument("-r",
                            "--rcfile",
                            metavar="<filename>",
                            dest="rc_file",
                            help="cqlrc file name. "
                                 "Default: ~/.cassandra/cqlshrc",
                            required=False)

        parser.add_argument("-s", "--ssl",
                            dest="use_ssl",
                            help="Use SSL/TLS encryption for client server "
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
        conn_params = tp.database.ConnectionParams()
        if self._args.rc_file:
            if not os.path.exists(self._args.rc_file):
                print("File '{}' not found.".format(self._args.rc_file))
                sys.exit(1)

            print("Reading configuration from '{}'..."
                  .format(self._args.rc_file), file=sys.stderr)
            conn_params = \
                tp.database.ConnectionParams.load_from_rcfile(
                    self._args.rc_file)

        # Apply switch settings.
        if self._args.host_ip:
            conn_params.host = self._args.host_ip
        if self._args.host_port:
            conn_params.port = self._args.host_port
        if self._args.username:
            conn_params.username = self._args.username
        if self._args.password_reqd:
            conn_params.password = password
        if self._args.use_ssl:
            conn_params.is_ssl_required = self._args.use_ssl
        if self._args.client_cert_file:
            conn_params.client_cert_file = self._args.client_cert_file
        if self._args.client_key_file:
            conn_params.client_key_file = self._args.client_key_file

        try:
            if self._args.dump_config or self._args.config_filename:
                # Construct the connection parameters
                db = tp.database.Db(conn_params)

                # Read current configuration from database
                current_config = db.get_current_config()

                if self._args.dump_config:
                    print(yaml.dump(current_config,
                                    default_flow_style=False))
                else:
                    config_filename = self._args.config_filename
                    logging.info("Reading config from '%s'", config_filename)
                    desired_config = tp.utils.load_yaml(config_filename)

                    # Generate ALTER statements for Keyspaces and Tables
                    alter_statements = tp.generator. \
                        generate_alter_statements(current_config,
                                                  desired_config)

                    print(alter_statements)

                    if alter_statements and self._args.run_quiet:
                        # Exit with code 1 if running in quiet mode and
                        # we have changes pending
                        sys.exit(1)
            else:
                self.get_arg_parser().print_usage()
        except Exception as ex:
            logging.exception(ex)


def main():
    """ Main function
    """
    cmd = TablePropertiesCli()
    cmd.execute(args=sys.argv[1:])

if __name__ == "__main__":
    main()
