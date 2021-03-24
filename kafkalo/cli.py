#!/usr/bin/env python3
# from alladmin import AllAdmin
from kafkalo.topics import KafkaAdmin
from kafkalo.schemas import SchemaAdmin
from kafkalo.inputparser import InputParser
from kafkalo.config import Config
from kafkalo.clients import MDSAdmin
from kafkalo.report import Report
import click
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from pathlib import Path
from kafkalo.cli_schema import schema as schema_group


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
@click.option(
    "--config",
    required=True,
    help="Config yaml file for kafkalo",
)
def sync(dry_run, config):
    """
    Synchronize Kafka config to YAML files
    """
    configuration = Config(filename=config)
    topic_admin, schema_admin, mds_admin = get_admin_clients(configuration)
    # Reconcile topics
    parser = InputParser(configuration.get_input_patterns())
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
@click.option(
    "--config",
    required=True,
    help="Config yaml file for kafkalo",
)
def plan(ctx, config):
    """
    Generate a plan. This is equivalent to sync --dry-run
    """
    ctx.invoke(sync, dry_run=True, config=config)


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
cli.add_command(schema_group)


def main():
    cli()


if __name__ == "__main__":
    main()
