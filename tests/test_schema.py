from kafkalo.schemas import Schema, SchemaAdmin
from kafkalo.inputparser import InputParser
from .mock_sr import MockSRClient

SAMPLE_PATH = ["tests/data/sample*.yaml"]
SAMPLE_SCHEMAS = [
    "tests/data/schema-key-update.json",
    "schema-key.json",
]


def test_compatibility():
    parser = InputParser(SAMPLE_PATH)
    schemas = parser.get_schemas()
    assert isinstance(schemas, list)
    s1 = schemas[0]
    assert isinstance(s1, Schema)
    assert s1.subject_name == "SKATA.VROMIA.POLY-key"
    assert s1.compatibility == "backward"
    s2 = schemas[1]
    assert isinstance(s1, Schema)
    assert s2.subject_name == "SKATA.VROMIA.POLY-value"
    assert s2.compatibility == "none"


def test_client():
    client = SchemaAdmin(MockSRClient({}))
    assert isinstance(client, SchemaAdmin)


def test_register_schema():
    client = SchemaAdmin(MockSRClient({}))
    parser = InputParser(SAMPLE_PATH)
    schemas = parser.get_schemas()
    schema1 = schemas[0]
    client.update_or_create_schema(schema1)


def test_populate_schema_cache():
    client = SchemaAdmin(MockSRClient({}))
    client._populate_subject_cache()
    assert len(client.subject_cache) == 0
    parser = InputParser(SAMPLE_PATH)
    schemas = parser.get_schemas()
    client.reconcile_schemas(schemas)
    client._populate_subject_cache()
    assert len(client.subject_cache) == 3


def test_get_subjects_to_update():
    client = SchemaAdmin(MockSRClient({}))
    parser = InputParser(SAMPLE_PATH)
    schemas = parser.get_schemas()
    to_update = client.get_subjects_to_update(schemas)
    assert len(to_update) == 3

    client.reconcile_schemas(schemas)
    to_update = client.get_subjects_to_update(schemas)
    assert len(to_update) == 0
