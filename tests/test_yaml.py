from yaml import load, dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

SAMPLE_PATH = "tests/data/sample.yaml"

from inputparser import InputParser
from topics import Topic
from schemas import Schema


def _read_file(fname=SAMPLE_PATH):
    with open(fname, "r") as fp:
        data = fp.read()
    return data


def test_load():
    data = _read_file()
    yamldata = load(data, Loader=Loader)
    assert isinstance(yamldata["clients"], list)
    assert isinstance(yamldata["topics"], list)

    for topic in yamldata["topics"]:
        assert isinstance(topic["topic"], str)
    for client in yamldata["clients"]:
        assert client["principal"].split(":")[0] in ["User", "Group"]


def test_inputparser_load():
    parser = InputParser("tests/data/sample.yaml")
    assert isinstance(parser, InputParser)


def test_inputparser_get_topics():
    parser = InputParser("tests/data/sample.yaml")
    topics = parser.get_topics()
    assert isinstance(topics, list)
    for topic in topics:
        assert isinstance(topic, Topic)
    # make sure we get the same data as in sample
    assert topics[0].name == "SKATA.VROMIA.POLY"
    assert topics[0].partitions == 6
    assert topics[0].replication_factor == 1
    assert topics[0].configs != None
    assert topics[0].configs["cleanup.policy"] == "delete"
    assert topics[0].configs["min.insync.replicas"] == 1
    assert topics[0].configs["retension.ms"] == 10000000
    assert topics[0].schema["value"]["fromFile"] == "tests/data/schema.json"
    assert topics[0].schema["key"]["fromFile"] == "tests/data/schema-key.json"


def test_inputparser_get_schemas():
    parser = InputParser("tests/data/sample.yaml")
    schemas = parser.get_schemas()
    for schema in schemas:
        assert isinstance(schema, Schema)
    assert schemas[0].subject_name == "SKATA.VROMIA.POLY-key"
    assert schemas[1].subject_name == "SKATA.VROMIA.POLY-value"
