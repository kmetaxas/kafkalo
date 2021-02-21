from schemas import Schema, SchemaAdmin
from inputparser import InputParser, DuplicateResourceException
from confluent_kafka.schema_registry import Schema as SRSchema
from mock_sr import MockSRClient
import json

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
    assert s1.compatibility == "none"
    s2 = schemas[1]
    assert isinstance(s1, Schema)
    assert s2.subject_name == "SKATA.VROMIA.POLY-value"
    assert s2.compatibility == "backward"


def test_client():
    client = SchemaAdmin(MockSRClient({}))


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
