#!/usr/bin/env python3
# from alladmin import AllAdmin
from topics import KafkaAdmin, Topic
from inputparser import InputParser

if __name__ == "__main__":
    kafka_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "kafkalo_consumer",
    }
    sr_config = {
        "url": "http://localhost:8081",
    }
    mds_config = {
        "url": "http://localhost:8090",
    }

    topic_admin = KafkaAdmin(kafka_config)
    parser = InputParser("tests/data/sample.yaml")
    topic_admin.list_topics()
    topic_admin.create_topics(parser.get_topics())
    topic_admin.delete_topics(parser.get_topics())
