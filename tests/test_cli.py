import table_properties.cli as cli

class TestTablePropertiesCli():
    def test_InvokeNoArgsUsage(self, capsys):
        cmd = cli.TablePropertiesCli()
        cmd.execute()
        out, _ = capsys.readouterr()
        assert out.startswith("usage:")