from confluent_kafka.admin import NewTopic, ConfigResource
from confluent_kafka import KafkaException
from typing import List

Type = ConfigResource.Type


class Topic(object):
    """
    Represents a single topic.
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

    def __init__(self, adminclient, consumer):
        """
        :adminclient is an instance of kafka AdminClient
        :adminclient is an instance of kafka Consumer
        """
        self.adminclient = adminclient
        self.consumer = consumer
        self.topics_cache = []
        self.dry_run_plan = {}

    def get_dry_run_plan(self):
        return self.dry_run_plan

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

    def _get_config_diff(self, before: dict, after: dict):
        """
        Return the changed configs
        """
        changes = {}
        for key in after.keys():
            if key not in before:
                changes[key] = after[key]
            else:
                if str(after[key]).strip() != str(before[key]).strip():
                    changes[key] = {"before": before[key], "after": after[key]}
        return changes

    def reconcile_topics(self, topics: List[Topic], dry_run=False, strict=False):
        """
        Reconcile configuration
        Create missing topics, and update configs.
        :dry_run will not update anything
        """
        current_medatada = self.list_topics()
        existing_topic_names = set(current_medatada.topics.keys())
        topic_names = set([x.name for x in topics])
        new_topic_names = topic_names - existing_topic_names
        # skipped_topic_names = existing_topic_names & topic_names
        topics_to_create = [x for x in topics if x.name in new_topic_names]

        topics_created, topics_failed = self.create_topics(
            topics_to_create, dry_run=dry_run
        )

        # now alter configs
        for topic in topics:
            if topic.name in topics_failed:
                continue
            if topic.configs:
                self.alter_config_for_topic(topic, dry_run=dry_run)
        # TODO If strict mode. delete topics not present in list.
        # Add param --zero-fucks-given to do that without user prompt to verify
        return (topics_created, topics_failed)

    def create_topics(self, topics: List[Topic], dry_run=None):
        """
        Create topics
        """
        if not topics:
            return ({}, {})
        # Lets create a dictionary of topic_name:Topic obj to make lookups
        # easier
        topics_dict = {x.name: x for x in topics}
        new_topics = [
            NewTopic(topic.name, topic.partitions, topic.replication_factor)
            for topic in topics
        ]
        # we get back a dict of {topic: future} that we can call the result on
        fs = self.adminclient.create_topics(
            new_topics, operation_timeout=10, validate_only=dry_run
        )

        topics_failed = {}
        topics_created = {}
        for topic_name, future in fs.items():
            topic = topics_dict[topic_name]
            try:
                future.result()
                topics_created[topic_name] = topic
                self._update_plan(
                    topic.name,
                    {
                        "topic": topic,
                        "create": "success",
                        "reason": None,
                    },
                )
            except Exception as e:
                topics_failed[topic_name] = {"topic": topic, "reason": str(e)}
                self._update_plan(
                    topic.name,
                    {
                        "topic": topic,
                        "create": "failed",
                        "reason": str(e),
                    },
                )

        return (topics_created, topics_failed)

    def _update_plan(self, topic: str, data: dict):
        """
        Update the plan for this topic with data dict
        """
        if topic not in self.dry_run_plan:
            self.dry_run_plan[topic] = data
        else:
            self.dry_run_plan[topic].update(data)

    def _sanitize_topic_config(self, config: dict):
        """
        Perform some basic sanity checks on the topic config and
        return a sanitized config dictionary
        """
        # Right now we will remove the following keys as we have run into an
        # issue where the broker respond with these in the existing config but
        # will not recogize them sent back.
        banned_settings = [
            "confluent.ssl.truststore.password",
            "confluent.ssl.truststore.location",
        ]
        for setting in banned_settings:
            if setting in config:
                del config[setting]
        return config

    def alter_config_for_topic(
        self, topic: Topic, dry_run=False, respect_existing_config=False
    ):
        """
        Alter the configuration of a single topic.
        :topic a Topic instance
        :respect_existing_config merge existing conig into new
        :dry_run perform dry-run only
        """

        new_config = {}
        # First get existing configs.. so really "old config" at this stage
        existing_config = {
            val.name: val.value
            for (key, val) in self.describe_topic(topic.name).items()
        }
        if respect_existing_config:
            new_config.update(existing_config)
        # And update with changed values to get real new config
        new_config.update(topic.configs)
        new_config = self._sanitize_topic_config(new_config)
        # get a config delta
        config_delta = self._get_config_diff(existing_config, new_config)

        resource = ConfigResource(restype=Type.TOPIC, name=topic.name)
        for key, value in new_config.items():
            resource.set_config(key, value)
        fs = self.adminclient.alter_configs(
            [resource], request_timeout=30, validate_only=dry_run
        )
        # check result

        configs_altered = []
        configs_failed = {}
        for res, future in fs.items():
            try:
                future.result()
                configs_altered.append(res)
            except Exception as e:
                configs_failed[res] = str(e)
        if topic.name not in self.dry_run_plan:
            self._update_plan(
                topic.name,
                {
                    "topic": topic,
                    "reason": None,
                    "config_delta": config_delta,
                },
            )
        self._update_plan(
            topic.name,
            {"configs_altered": configs_altered, "configs_failed": configs_failed},
        )
        return (configs_altered, configs_failed, config_delta)

    def describe_topic(self, topic: str):
        """
        Return a list of configs for the provided topic name
        """
        resource = ConfigResource(restype=Type.TOPIC, name=topic)
        fs = self.adminclient.describe_configs([resource])
        if len(fs) != 1:
            return (
                f"describe_configs for topic {topic} did not return a single response."
            )
        for res, future in fs.items():
            try:
                configs = future.result()
                # for config in configs.values():
                #    print(f"Topic {topic} -> {config.name} = {config.value}")
                return configs
            except KafkaException as e:
                print(f"Failed to to describe config for {res} with error {e}")
                return {}

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
