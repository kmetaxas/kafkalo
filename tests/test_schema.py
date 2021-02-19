from schemas import Schema
from inputparser import InputParser, DuplicateResourceException


SAMPLE_PATH = ["tests/data/sample*.yaml"]


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
