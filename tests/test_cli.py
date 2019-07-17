# pylint: disable=missing-docstring,invalid-name,no-self-use
import argparse

import pytest

import table_properties as tp


# pylint: disable=too-few-public-methods
class TestTablePropertiesCli():
    def test_InvokeNoArgsUsage(self, capsys):
        cmd = tp.cli.TablePropertiesCli()
        cmd.execute([])
        out, _ = capsys.readouterr()
        assert out.startswith("usage:")

    def test_InvokePrintVersion(self, capsys):
        cmd = tp.cli.TablePropertiesCli()
        # Version switch triggers SystemExit
        with pytest.raises(SystemExit):
            cmd.execute(["-v"])
        out, _ = capsys.readouterr()
        assert out.startswith("table-properties")

    def test_InvokeDumpConfig(self, capsys):
        cmd = tp.cli.TablePropertiesCli()
        cmd.execute(["-d"])
        out, _ = capsys.readouterr()
        assert out.strip().startswith("keyspaces: [")

    def test_InvokeLoadConfig(self, capsys):
        cmd = tp.cli.TablePropertiesCli()
        cmd.execute(["tests/configs/excalibur_unchanged.yaml"])
        out, _ = capsys.readouterr()
        assert out.strip() == ""

    def test_InvokeLoadRc(self, capsys):
        cmd = tp.cli.TablePropertiesCli()
        cmd.execute(["-r", "tests/setup/cqlshrc", "-d"])
        out, _ = capsys.readouterr()
        assert out.strip().startswith("keyspaces: [")

    def test_ArgParser(self):
        parser = tp.cli.TablePropertiesCli.get_arg_parser()
        assert parser is not None
        assert isinstance(parser, argparse.ArgumentParser)
