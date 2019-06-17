#!/usr/bin/env python3

import argparse
import logging
import os
import sys

import yaml

import table_properties as tp

class TablePropertiesCmd():
    def parse_arguments(self, args=sys.argv[1:]):
        return self.get_arg_parser().parse_args(args)

    def get_arg_parser(self):
        """
        Parse arguments
        """
        parser = argparse.ArgumentParser(prog="table-properties", 
            description="""Compare actual table properties against desired 
                           properties and create an ALTER statement.""")
        parser.add_argument(metavar='<config file>', type=argparse.FileType('r'),
                            dest="config_filename", help='YAML configuration file')

        return parser

    def execute(self):
        tp.utils.setup_logging()

        self.args = self.parse_arguments()
        config_filename = self.args.config_filename.name
        logging.info(f"Reading config from '{config_filename}'")

        # Read current configuration from database
        current_config = tp.db.get_current_config()

        # Load the desired configuration from file
        desired_config = tp.utils.load_config(config_filename)

        # Generate ALTER statements for Keyspaces and Tables
        alter_statements = tp.generator. \
            generate_alter_statements(current_config, desired_config)

        print(alter_statements)


def main():
    tpc = TablePropertiesCmd()
    tpc.execute()


if __name__ == "__main__":
    main()
