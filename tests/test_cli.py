# pylint: disable=missing-docstring,invalid-name,no-self-use
import table_properties.cli as cli


# pylint: disable=too-few-public-methods
class TestTablePropertiesCli():
    def test_InvokeNoArgsUsage(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute()
        out, _ = capsys.readouterr()
        assert out.startswith("usage:")
