from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from typing import List

Type = ConfigResource.Type


class Topic(object):
    """
    Represents a single topic.
    Manages itself
    """

    def __init__(self, name, partitions, replication_factor, configs=None, schema=None):
        self.name = name
        self.partitions = partitions
        self.replication_factor = replication_factor
        self.configs = configs
        self.schema = schema


class KafkaAdmin(object):
    """
    A Kafka AdminClient wrapper
    Can manage topics
    """

    def __init__(self, kafka_config):
        """
        kafka_config a dictionary as used by librdkafka (and confluent-python)
        """
        self.adminclient = AdminClient(kafka_config)
        self.consumer = Consumer(kafka_config)
        self.topics_cache = []

    def list_topics(self):
        """
        Retrieve list of topics from Kafka
        """
        metadata = self.consumer.list_topics()
        topics_cache = []
        # Set a new topic cache
        for topic, topic_metadata in metadata.topics.items():
            topics_cache.append(topic)
            self.topics_cache = topics_cache
        # return the complete metadata object
        return metadata

    def create_topics(self, topics: List[Topic], dry_run=False):
        """
        Create Topics from a list
        """
        new_topics = [
            NewTopic(topic.name, topic.partitions, topic.replication_factor)
            for topic in topics
        ]
        # we get back a dict of {topic: future} that we can call the result on
        fs = self.adminclient.create_topics(
            new_topics, operation_timeout=10, validate_only=dry_run
        )

        for topic, future in fs.items():
            try:
                future.result()
                print(f"Created topic {topic}")
            except Exception as e:
                print(f"Failed to create topic {topic} with: {e}")

        # TODO alter configs now

    def alter_config_for_topic(self, topic, configs):
        """
        Alter the configuration of a single topic
        """

        # First get existing configs.. so really "old config" at this stage
        new_config = {
            val.name: val.value for (key, val) in self.describe_topic(topic).items()
        }
        # And update with changed values to get real new config
        new_config.update(configs)

        resource = ConfigResource(restype=Type.TOPIC, name=topic)
        for key, value in new_config.items():
            resource.set_config(key, value)
        fs = self.adminclient.alter_configs([resource])
        # check result
        for res, future in fs.items():
            try:
                future.result()
                print(f"{res} successfuly altered")
            except Exception as e:
                print(f"Failed to set config: {e}")

    def describe_topic(self, topic: str):
        """
        Return a list of configs for the provided topic name
        """
        resource = ConfigResource(restype=Type.TOPIC, name=topic)
        fs = self.adminclient.describe_configs([resource])
        if len(fs) != 1:
            return f"describe_configs for a topic did not return a single response."
        for res, future in fs.items():
            try:
                configs = future.result()
                for config in configs.values():
                    print(f"Config {config.name} = {config.value}")
                return configs
            except KafkaException as e:
                print(f"Failed to to describe config for {res}")
                return None

    def delete_topics(self, topics: List[Topic], dry_run=False):
        """
        Delete topics from a list
        """
        doomed_topics = [topic.name for topic in topics]
        # we get back a dict of {topic: future} that we can call the result on
        fs = self.adminclient.delete_topics(
            doomed_topics,
            operation_timeout=30,
        )

        for topic, future in fs.items():
            try:
                future.result()
                print(f"Deleted topic {topic}")
            except Exception as e:
                print(f"Failed to delete topic {topic} with: {e}")
