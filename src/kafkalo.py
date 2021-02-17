#!/usr/bin/env python3
# from alladmin import AllAdmin
from topics import KafkaAdmin, Topic
from schemas import SchemaAdmin, Schema
from inputparser import InputParser
import yaml
from config import Config
from clients import MDSAdmin
import click


@click.group()
def cli():
    pass


@click.command()
def sync():
    """
    Synchronize Kafka config to YAML files
    """
    config = Config()
    topic_admin, schema_admin, mds_admin = get_admin_clients(config)
    # Reconcile topics
    parser = InputParser("tests/data/sample.yaml")
    topic_admin.reconcile_topics(parser.get_topics())
    # Reconcile schemas
    schemas = parser.get_schemas()
    schema_admin.reconcile_schemas(schemas)

    # TODO no reconcile yet for Clients...
    # mds_admin.do_consumer_for("SKATA", "arcanum")
    mds_admin.reconcile_roles(parser.get_clients())


def get_admin_clients(config):
    topic_admin = KafkaAdmin(config.get_kafka_config())
    schema_admin = SchemaAdmin(config.get_sr_config())
    mds_admin = MDSAdmin(config.get_mds_config())
    return (topic_admin, schema_admin, mds_admin)


cli.add_command(sync)

if __name__ == "__main__":
    config = Config()
    cli()

    # topic_admin.list_topics()
    # topic_admin.reconcile_topics(parser.get_topics())
    # topic_admin.delete_topics(parser.get_topics())
    # topic_admin.alter_config_for_topic(
    #    "SKATA.VROMIA.POLY",
    #    {
    #        "cleanup.policy": "compact",
    #    },
    # )

    # topic_admin.describe_topic("SKATA.VROMIA.POLY")
    # Do schema
    # print(mds_admin.get_kafka_cluster_id())
    # print(mds_admin.get_rolebinding_for_user("arcanum"))
    # mds_admin.do_consumer_for("SKATA", "arcanum")
