import pytest


from kafkalo.inputparser import InputParser, DuplicateResourceException
from kafkalo.topics import Topic
from kafkalo.schemas import Schema

SAMPLE_PATH = ["tests/data/sample*.yaml"]


def test_inputparser_load():
    parser = InputParser(SAMPLE_PATH)
    assert isinstance(parser, InputParser)


def test_inputparser_get_topics():
    parser = InputParser(SAMPLE_PATH)
    topics = parser.get_topics()
    assert isinstance(topics, list)
    for topic in topics:
        assert isinstance(topic, Topic)
    # make sure we get the same data as in sample
    assert topics[0].name == "SKATA.VROMIA.POLY"
    assert topics[0].partitions == 6
    assert topics[0].replication_factor == 1
    assert topics[0].configs is not None
    assert topics[0].configs["cleanup.policy"] == "delete"
    assert topics[0].configs["min.insync.replicas"] == 1
    assert topics[0].configs["retention.ms"] == 10000000
    assert topics[0].schema["value"]["fromFile"] == "tests/data/schema.json"
    assert topics[0].schema["value"]["compatibility"] == "BACKWARD"
    assert topics[0].schema["key"]["fromFile"] == "tests/data/schema-key.json"
    assert topics[0].schema["key"]["compatibility"] == "NONE"


def test_inputparser_get_schemas():
    parser = InputParser(SAMPLE_PATH)
    schemas = parser.get_schemas()
    for schema in schemas:
        assert isinstance(schema, Schema)
    assert schemas[0].subject_name == "SKATA.VROMIA.POLY-key"
    assert schemas[1].subject_name == "SKATA.VROMIA.POLY-value"
    # Topic 1 has not compatibility set
    assert schemas[0].compatibility == "none"
    assert schemas[1].compatibility == "backward"
    assert schemas[2].compatibility is not None


def test_resolve_patterns():
    patterns = [
        "tests/data/patterns/*.yaml",
    ]
    parser = InputParser(patterns)
    assert isinstance(parser, InputParser)

    patterns2 = patterns + ["tests/data/patterns/faildir/*.yaml"]
    with pytest.raises(DuplicateResourceException) as e:
        parser = InputParser(patterns2)
        assert "already declared" in str(e.value)

    pattern3 = ["tests/data/patterns/sample.yaml"]
    parser = InputParser(pattern3)
    assert isinstance(parser, InputParser)
    assert "tests/data/patterns/sample.yaml" in parser.filenames
    assert len(parser.filenames) == 1
