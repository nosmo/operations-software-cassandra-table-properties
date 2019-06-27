import argparse
import logging
import os
import sys

import table_properties as tp

class TablePropertiesCli():
    """Command-line interface class
    """
    def parse_arguments(self, args=sys.argv[1:]) -> argparse.Namespace:
        """ Parse shell parameters

        Returns:
            Namespace object exposing parameters as attributes
        """
        return self.get_arg_parser().parse_args(args)

    def get_arg_parser(self) -> argparse.ArgumentParser:
        """Set up CLI arguments in parser

        Returns:
            argparse.ArgumentParser
        """
        parser = argparse.ArgumentParser(prog=tp.PROG_NAME,
            description="""Compare actual Cassandra keyspace and table
                           configuration against desired configuration
                           and create ALTER statements to change the current
                           property values to the desired ones.""")

        parser.add_argument(metavar="<filename>",
                            nargs="?",
                            dest="config_filename",
                            help="Desired configuration YAML file")

        parser.add_argument("-f", "--force",
                            dest="force_overwrite",
                            help="Overwrite dump file if it exists.",
                            action="store_true")

        parser.set_defaults(force_overwrite=False)

        parser.add_argument("-o",
                            "--output",
                            metavar="<filename>",
                            dest="dump_file",
                            help="Dump current configuration settings to file",
                            required=False)

        parser.add_argument("-v",
                            "--version",
                            action="version",
                            version="{} {}".format(tp.PROG_NAME,
                                tp.PROG_VERSION))

        return parser

    def execute(self) -> None:
        """Execute applicaton
        """
        # Modify root logger settings
        tp.utils.setup_logging()

        config_filename = None
        app_dir = tp.utils.get_app_folder()

        # Load the desired configuration from file
        self.args = self.parse_arguments()
        
        try:
            # Read current configuration from database
            current_config = tp.db.get_current_config()

            if self.args.dump_file:
                try:
                    tp.utils.write_yaml(self.args.dump_file, current_config,
                                        self.args.force_overwrite)
                except Exception as ex:
                    logging.exception(ex)

            if self.args.config_filename:
                config_filename = self.args.config_filename
                logging.info("Reading config from '%s'", config_filename)
                desired_config = tp.utils.load_yaml(config_filename)

                config_schema_file = os.path.join(app_dir, "schemas", "config.json")
                config_schema = tp.utils.load_json(config_schema_file)
                if not tp.utils.validate_schema(config_schema, desired_config):
                    print("""Schema validation of the desired config file failed.\n
                            See log for more information.""")

                # Generate ALTER statements for Keyspaces and Tables
                alter_statements = tp.generator. \
                    generate_alter_statements(current_config, desired_config)

                print(alter_statements)
            if not (self.args.dump_file or self.args.config_filename):
                self.get_arg_parser().print_usage()
        except Exception as ex:
            logging.exception(ex)
            print(ex)

def main():
    tpc = TablePropertiesCli()
    tpc.execute()

if __name__ == "__main__":
    main()
