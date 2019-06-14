import json

import jsonschema
import yaml

def test_data_centers():
    with open('schemas/data_centers.json', 'r') as f:
        schema = json.load(f)

    print(schema)

    with open("configs/data_centers.yaml") as f:
        config = yaml.safe_load(f)

    print(config)

    try:
        jsonschema.Draft7Validator(schema).validate(instance=config)
        print('Is valid!')
    except Exception as ex:
        print('Is invalid')
        print(ex)

def test_tables():
    with open('schemas/tables.json', 'r') as f:
        schema = json.load(f)

    print(schema)

    with open("configs/tables.yaml") as f:
        config = yaml.safe_load(f)

    print(config)
    
    try:
        jsonschema.Draft7Validator(schema).validate(instance=config)
        print('Is valid!')
    except Exception as ex:
        print('Is invalid')
        print(ex)

def test_all():
    with open('schemas/master.json', 'r') as f:
        schema = json.load(f)

    print(schema)

    with open("configs/table-properties.yaml") as f:
        config = yaml.safe_load(f)

    print(config)


    try:
        jsonschema.Draft7Validator(schema).validate(instance=config)
        print('Is valid!')
    except Exception as ex:
        print('Is invalid')
        print(ex)

# test_data_centers()
# test_tables()
test_all()