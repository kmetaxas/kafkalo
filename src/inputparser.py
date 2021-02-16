from yaml import load

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from topics import Topic
from schemas import Schema


class InputParser(object):
    """
    PArse the input YAML and feed it to the Admin
    """

    def __init__(self, input_file):
        with open(input_file, "r") as fp:
            self.data = load(fp.read(), Loader=Loader)

    def get_topics(self):
        """
        Return a list o Topic objects found in the input YAML.
        """
        # TODO make this a generator as they number of topics could be big.
        resp = []
        for topicdata in self.data["topics"]:
            topic = Topic(
                name=topicdata["topic"],
                partitions=topicdata["partitions"],
                replication_factor=topicdata["replication_factor"],
                configs=topicdata.get("configs", None),
                schema=topicdata.get("schema", None),
            )
            resp.append(topic)
        return resp

    def get_schemas(self):
        topics = self.get_topics()
        schemas = []
        for topic in topics:
            if topic.schema:
                if "key" in topic.schema:
                    filename = topic.schema["key"].get("fromFile", None)
                    if filename:
                        with open(filename, "r") as fp:
                            schema_data = fp.read()
                            schema = Schema(
                                subject_name=f"{topic.name}-key", schema=schema_data
                            )
                            schema.compatibility = topic.schema["key"].get(
                                "compatibility", None
                            )
                            schemas.append(schema)
                if "value" in topic.schema:
                    filename = topic.schema["value"].get("fromFile", None)
                    if filename:
                        with open(filename, "r") as fp:
                            schema_data = fp.read()
                            schema = Schema(
                                subject_name=f"{topic.name}-value", schema=schema_data
                            )
                            schema.compatibility = topic.schema["value"].get(
                                "compatibility", None
                            )
                            schemas.append(schema)
        return schemas

    def get_clients(self):
        """
        Get the the client configuration
        """
        pass

    def create_rolebindings(self):
        """
        Create an rolebindigns defined in the YAML
        """
        pass
