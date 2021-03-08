from yaml import load
from typing import List

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from kafkalo.topics import Topic
from kafkalo.schemas import Schema
from kafkalo.clients import Client
from pathlib import Path


class DuplicateResourceException(Exception):
    """
    Genarated with a yaml contains a resource defiition already declared in
    somewhere else.
    """

    pass


class InputParser(object):
    """
    Parse the input YAML and feed it to the Admin
    """

    def __init__(self, patterns: List[str]):
        self.filenames = self._resolve_patterns(patterns)
        self.data = self._load_and_merge(self.filenames)

    def _load_and_merge(self, filenames: List[str]):
        """
        Load files and merge them into a big dictionary
        """
        merged_data = {}
        for filename in filenames:
            with open(filename, "r") as fp:
                data = load(fp.read(), Loader=Loader)
                # Now merge the keys
                for key, values in data.items():
                    if key not in merged_data:
                        merged_data[key] = []
                    # Test if a value already exists in merged data. This would
                    # indicate multiple definitions for the same resource and
                    # would overwite one unpredictable
                    for value in values:
                        if value in merged_data[key]:
                            raise DuplicateResourceException(
                                f"Resource {value} already declared elsewhere"
                            )
                    merged_data[key] += values
        return merged_data

    def _resolve_patterns(self, patterns: List[str]):
        """
        Iterate of the list of glob patterns and return a list of files to load
        """

        filenames = []
        for pattern in patterns:
            p = Path(pattern)
            # TODO handle case where we get a specific file and not a glob
            # pattern as a stem
            if "*" not in p.stem:
                filenames.append(pattern)
            else:
                filenames += [x for x in p.parent.glob(p.stem) if not Path(x).is_dir()]
        return filenames

    def _make_schema_dict(self, topic_data):
        """
        Given a topic definition dictionary, get the 'key' and 'schema' keys
        and merge them into a new 'schema' key suitable for Topic object
        """
        schema = {}

        if "key" in topic_data:
            schema["key"] = {
                "fromFile": topic_data["key"]["schema"],
                "compatibility": topic_data["key"].get("compatibility", None),
            }
        if "value" in topic_data:
            schema["value"] = {
                "fromFile": topic_data["value"]["schema"],
                "compatibility": topic_data["value"].get("compatibility", None),
            }
        if schema.keys():
            return schema
        else:
            return None

    def get_topics(self):
        """
        Return a list o Topic objects found in the input YAML.
        """
        # TODO make this a generator as they number of topics could be big.
        resp = []
        for topicdata in self.data["topics"]:
            schema_data = self._make_schema_dict(topicdata)
            topic = Topic(
                name=topicdata["name"],
                partitions=topicdata["partitions"],
                replication_factor=topicdata["replication_factor"],
                configs=topicdata.get("configs", None),
                schema=schema_data,
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
                            compatibility = topic.schema["key"].get(
                                "compatibility", None
                            )
                            schema = Schema(
                                subject_name=f"{topic.name}-key",
                                schema=schema_data,
                                compatibility=compatibility,
                            )
                            schemas.append(schema)
                if "value" in topic.schema:
                    filename = topic.schema["value"].get("fromFile", None)
                    if filename:
                        with open(filename, "r") as fp:
                            schema_data = fp.read()
                            compatibility = topic.schema["value"].get(
                                "compatibility", None
                            )
                            schema = Schema(
                                subject_name=f"{topic.name}-value",
                                schema=schema_data,
                                compatibility=compatibility,
                            )
                            schemas.append(schema)
        return schemas

    def get_schemas_as_dict(self):
        """
        Get the schemas as a dictionary with the subject_name being the key
        """
        schema_list = self.get_schemas()
        schemas = {}
        for schema in schema_list:
            schemas[schema.subject_name] = schema
        return schemas

    def get_clients(self):
        """
        Get the the client configuration
        """
        if "clients" not in self.data:
            return None
        clients = []
        for client_dict in self.data["clients"]:
            client = Client(
                principal=client_dict["principal"],
                consumer_for=client_dict.get("consumer_for", None),
                producer_for=client_dict.get("producer_for", None),
                resourceowner_for=client_dict.get("resourceowner_for", None),
            )
            clients.append(client)
        return clients
