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
    assert topics[0].schema["value"]["fromFile"] == "schema.json"
    assert topics[0].schema["value"]["compatibility"] == "NONE"
    assert topics[0].schema["key"]["fromFile"] == "schema-key.json"
    assert topics[0].schema["key"]["compatibility"] == "BACKWARD"


def test_inputparser_get_schemas():
    parser = InputParser(SAMPLE_PATH)
    schemas = parser.get_schemas_as_dict()
    for schema in schemas.values():
        assert isinstance(schema, Schema)
    assert schemas["SKATA.VROMIA.POLY-key"].subject_name == "SKATA.VROMIA.POLY-key"
    assert schemas["SKATA.VROMIA.POLY-value"].subject_name == "SKATA.VROMIA.POLY-value"
    # Topic 1 has not compatibility set
    assert schemas["SKATA.VROMIA.POLY-value"].compatibility == "none"
    assert schemas["SKATA.VROMIA.POLY-key"].compatibility == "backward"
    assert schemas["SKATA.VROMIA.LIGO-key"].compatibility is None


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


def test_clients():
    """
    test client
    """
    parser = InputParser(SAMPLE_PATH)
    client_list = parser.get_clients()
    assert isinstance(client_list, list)

    clients = {x.principal: x for x in client_list}
    assert "User:poutanaola" in clients
    assert "Group:malakes" in clients
    assert "User:produser" in clients
