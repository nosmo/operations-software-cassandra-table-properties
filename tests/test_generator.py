import table_properties as tp
import json

class TestGenerator:
    def test_isPrimitive(self):
        assert tp.generator.isPrimitive(True) == True

    def test_excalibur_increase_replicas(self):
        # Load the mock config
        with open("./tests/mocks/excalibur.json", "r") as f:
            current_config = json.load(f)
        # Load the YAML
        desired_config = tp.utils.load_yaml("./tests/configs/excalibur_incr_dcs.yaml")

        # Validate output
        tp.generator.generate_alter_statements(current_config, desired_config)