#!/usr/bin/env python3

import argparse
import logging
import os
import sys

import table_properties.utils

DEFAULT_CONFIG_RELATIVE_PATH = "config/table-properties.yaml"
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class TablePropertiesCmd():
    def __init(self):
        self.logger = None

    def setup_logging(self):
        logging.basicConfig(format=DEFAULT_LOG_FORMAT, level=logging.DEBUG)

        # Color codes http://www.tldp.org/HOWTO/Bash-Prompt-HOWTO/x329.html
        logging.addLevelName(  # cyan
            logging.DEBUG, "\033[36m%s\033[0m" %
            logging.getLevelName(logging.DEBUG))
        logging.addLevelName(  # green
            logging.INFO, "\033[32m%s\033[0m" %
            logging.getLevelName(logging.INFO))
        logging.addLevelName(  # yellow
            logging.WARNING, "\033[33m%s\033[0m" %
            logging.getLevelName(logging.WARNING))
        logging.addLevelName(  # red
            logging.ERROR, "\033[31m%s\033[0m" %
            logging.getLevelName(logging.ERROR))
        logging.addLevelName(  # red background
            logging.CRITICAL, "\033[41m%s\033[0m" %
            logging.getLevelName(logging.CRITICAL))

        root = logging.getLogger('CassandraTableProperties')

        return root

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
        self.logger = self.setup_logging()

        self.args = self.parse_arguments()

        self.logger.info(f"Reading config from '{self.args.config_filename.name}'")


def main():
    tpc = TablePropertiesCmd()
    tpc.execute()


if __name__ == "__main__":
    main()
