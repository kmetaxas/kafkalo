from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient


class AllAdmin(object):
    """
    Admin all the things
    """

    def __init__(self, kafka_config, sr_config, mds_config):
        """
        kafka_config dictionary
        `sr_config` dictionary to connect to Schema registry
        `mds_config` dictionary to connect to MDS
        """
        self.kafka_config = kafka_config
        self.sr_config = sr_config
        self.mds_config = mds_config

        self.kafka_client = self._connect_kafka(kafka_config)
        self.sr_client = self._connect_sr(sr_config)

    def _connect_kafka(self, kafka_config):
        """
        Create a KafkaAdmin client
        """
        client = AdminClient(kafka_config)
        return client

    def _connect_sr(self, sr_config):
        """
        Create a Schema registry client
        """
        client = SchemaRegistryClient(sr_config)
        return client

    def create_topics(self, topic_config):
        """
        Create topics in topic_config dictionary
        """
        pass
