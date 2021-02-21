#!/usr/bin/env python3
# from alladmin import AllAdmin
from topics import KafkaAdmin, Topic
from schemas import SchemaAdmin, Schema
from inputparser import InputParser
import yaml
from config import Config
from clients import MDSAdmin
from report import Report
import click
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient


@click.group()
def cli():
    pass


@click.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Don't change anything but do a dry run",
)
def sync(dry_run):
    """
    Synchronize Kafka config to YAML files
    """
    config = Config()
    topic_admin, schema_admin, mds_admin = get_admin_clients(config)
    # Reconcile topics
    parser = InputParser(config.get_input_patterns())
    topic_admin.reconcile_topics(parser.get_topics(), dry_run=dry_run)
    topics_context = topic_admin.get_dry_run_plan()
    # Reconcile schemas
    schemas = parser.get_schemas()
    schema_admin.reconcile_schemas(schemas, dry_run=dry_run)
    schema_context = schema_admin.get_dry_run_plan()

    # TODO no reconcile yet for Clients...
    # mds_admin.do_consumer_for("SKATA", "arcanum")
    mds_admin.reconcile_roles(parser.get_clients(), dry_run=dry_run)
    if dry_run:
        client_context = mds_admin.get_dry_run_plan()
        report = Report(
            client_context=client_context,
            schema_context=schema_context,
            topics_context=topics_context,
        )
        print(report.render())


@click.command()
@click.pass_context
def plan(ctx):
    """
    Generate a plan. This is equivalent to sync --dry-run
    """
    ctx.invoke(sync, dry_run=True)


def get_admin_clients(config):

    kafka_config = config.get_kafka_config()
    adminclient = AdminClient(kafka_config)
    consumer = AdminClient(kafka_config)
    topic_admin = KafkaAdmin(adminclient, consumer)

    sr_client = SchemaRegistryClient(config.get_sr_config())
    schema_admin = SchemaAdmin(sr_client)
    mds_admin = MDSAdmin(config.get_mds_config())
    return (topic_admin, schema_admin, mds_admin)


cli.add_command(sync)
cli.add_command(plan)

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
