# pylint: disable=missing-docstring,invalid-name,no-self-use
import argparse

import pytest

import src.cli as cli


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
        assert out.startswith("table-properties")

    def test_invoke_dump_config(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute(["-d"])
        out, _ = capsys.readouterr()
        assert out.strip() == ""

    def test_invoke_load_config(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute(["tests/configs/excalibur_unchanged.yaml"])
        out, _ = capsys.readouterr()
        assert out.strip() == ""

    def test_invoke_load_rc(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute(["-r", "tests/setup/cqlshrc", "-d"])
        out, _ = capsys.readouterr()
        assert out.strip() == ""

    def test_invoke_load_rc_nonexisting(self, capsys):
        cmd = cli.TablePropertiesCli()
        with pytest.raises(SystemExit):
            cmd.execute(["-r", "tests/setup/cqlshrc12345", "-d"])
        out, _ = capsys.readouterr()
        assert out.strip().startswith(
            "File 'tests/setup/cqlshrc12345' not found")

    def test_argparser(self):
        parser = cli.TablePropertiesCli.get_arg_parser()
        assert parser is not None
        assert isinstance(parser, argparse.ArgumentParser)
