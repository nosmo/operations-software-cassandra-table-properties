# pylint: disable=missing-docstring, no-self-use
import argparse
import os

import pytest

from tableproperties import cli, tests, PROG_NAME


# pylint: disable=too-few-public-methods
class TestTablePropertiesCli():
    def test_invoke_no_args_usage(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute([])
        out, _ = capsys.readouterr()
        assert out.startswith("usage:")

    def test_invoke_print_version(self, capsys):
        cmd = cli.TablePropertiesCli()
        # Version switch triggers SystemExit
        with pytest.raises(SystemExit):
            cmd.execute(["-v"])
        out, _ = capsys.readouterr()
        assert out.startswith(PROG_NAME)

    @pytest.mark.skip(reason="test requires local cassandra instance")
    def test_invoke_dump_config(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute(["-d"])
        out, _ = capsys.readouterr()
        assert out.strip() != ""

    @pytest.mark.skip(reason="test requires local cassandra instance")
    def test_invoke_load_config(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute([
            os.path.join(tests.TEST_ROOT, "configs/excalibur_unchanged.yaml")
        ])
        out, _ = capsys.readouterr()
        assert out.strip() == ""

    @pytest.mark.skip(reason="test requires local cassandra instance")
    def test_invoke_load_rc(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute(
            ["-c", os.path.join(tests.TEST_ROOT, "setup/cqlshrc"), "-d"])
        out, _ = capsys.readouterr()
        assert out.strip() != ""

    def test_invoke_load_rc_nonexisting(self, capsys):
        test_file = os.path.join(tests.TEST_ROOT, "setup/cqlshrc12345")
        cmd = cli.TablePropertiesCli()
        with pytest.raises(SystemExit):
            cmd.execute(["-c", test_file, "-d"])
        out, _ = capsys.readouterr()
        assert out.strip() == "File '{}' not found.".format(test_file)

    def test_argparser(self):
        parser = cli.TablePropertiesCli.get_arg_parser()
        assert parser is not None
        assert isinstance(parser, argparse.ArgumentParser)
