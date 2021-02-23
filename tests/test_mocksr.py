from confluent_kafka.schema_registry import Schema, RegisteredSchema
from confluent_kafka.schema_registry.error import SchemaRegistryError
from .mock_sr import MockSRClient
import json
import pytest

SAMPLE_PATH = ["tests/data/sample*.yaml"]
SAMPLE_SCHEMAS = [
    "tests/data/schema-key-update.json",
    "schema-key.json",
]
SCHEMA1 = """
{
    "type" : "record",
    "name" : "userInfo",
    "namespace" : "my.example",
    "fields" : [{"name" : "age", "type" : "int", "default" : -1}]
    }
"""
SCHEMA2 = """
{
    "type" : "record",
    "name" : "userInfo",
    "namespace" : "my.example",
    "fields" : [{"name" : "age", "type" : "int", "default" : -1},
    {"name": "height","type":"int"}]
    }
"""


def test_get_subjects():
    client = MockSRClient({})
    assert len(client.get_subjects()) == 0
    # Bypass add methods to directly add subjects and test `get_subjects()`
    client.subjects["test1"] = {
        "versions": {1: "not_a_schema"},
    }
    assert len(client.get_subjects()) == 1
    for name in ["test" + str(x) for x in range(2, 11)]:
        client.subjects[name] = {
            "versions": {1: "not_a_schema"},
        }
    assert len(client.get_subjects()) == 10


def test_set_compatibility():
    client = MockSRClient({})
    # Starting default
    assert client.compatibility == "BACKWARD"
    # Set global default
    client.set_compatibility(subject_name=None, level="FORWARD")
    assert client.compatibility == "FORWARD"
    assert client.get_compatibility() == {"compatibilityLevel": "FORWARD"}
    client.set_compatibility(subject_name=None, level="BACKWARD")
    assert client.get_compatibility() == {"compatibilityLevel": "BACKWARD"}

    # set compat for a topic
    name = "test-key"

    with pytest.raises(SchemaRegistryError) as e:
        assert client.get_compatibility(name) == {"compatibilityLevel": "BACKWARD"}
    assert e.value.error_message == "Subject not found"
    assert e.value.error_code == 40401
    schema_id = client.register_schema(name, Schema(json.dumps(SCHEMA1), "AVRO"))
    assert schema_id == 1
    client.set_compatibility(subject_name=name, level="NONE")
    assert client.get_compatibility(name) == {"compatibilityLevel": "NONE"}
    assert client.get_compatibility() == {"compatibilityLevel": "BACKWARD"}


def test_register_schema():
    name = "test1-key"
    client = MockSRClient({})
    schema_id = client.register_schema(name, Schema(json.dumps(SCHEMA1), "AVRO"))
    assert schema_id == 1
    assert name in client.subjects
    subject = client.subjects[name]
    assert "versions" in subject
    assert len(subject["versions"]) == 1
    assert subject["versions"][1] == schema_id

    # register new version
    schema = Schema(json.dumps(SCHEMA2), "AVRO")
    schema_id = client.register_schema(name, schema)
    subject = client.subjects[name]
    assert schema_id == 2
    assert len(subject["versions"]) == 2
    assert subject["versions"][2] == schema_id
    assert client.schemas[schema_id] == schema


def test_lookup_schema():
    name = "test1-key"
    schema2 = Schema(json.dumps(SCHEMA2), "AVRO")
    client = MockSRClient({})
    # Lookup a non existent schema
    with pytest.raises(SchemaRegistryError) as e:
        client.lookup_schema("DOES_NOT_EXIST", schema2)
    assert e.value.error_code == 40403
    # add a schema and try again
    schema_id = client.register_schema(name, schema2)
    assert schema_id == 1
    found = client.lookup_schema(name, schema2)
    assert isinstance(found, RegisteredSchema)
