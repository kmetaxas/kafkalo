from yaml import load, dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

SAMPLE_PATH = "tests/data/sample.yaml"


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
