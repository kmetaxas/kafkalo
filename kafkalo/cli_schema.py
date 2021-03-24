import click
from kafkalo.config import Config
from confluent_kafka.schema_registry import SchemaRegistryClient
from kafkalo.schemas import SchemaAdmin, Schema


@click.group()
def schema():
    pass


@schema.command()
@click.option("--config", required=True, help="Config yaml file for kafkalo")
@click.option("--subject", required=True, help="Subject name")
@click.option("--schema-file", required=True, help="File containing schema")
def check_exists(subject, schema_file, config):
    """
    Test if schema in provided file is already registered in provided Subject
    """
    print(f"Will check {subject} for file {schema_file}")
    configuration = Config(filename=config)
    sr_client = SchemaRegistryClient(configuration.get_sr_config())
    schema_admin = SchemaAdmin(sr_client)
    with open(schema_file, "r") as fp:
        schema = Schema(subject, fp.read())
        success, response = schema_admin.lookup_schema(schema)
        if success:
            print(
                f"Schema found: SUBJECT: {response.subject}, ID:{response.schema_id} VERSION: {response.version}"
            )
        else:
            print(f"Schema not found. (Registry responded with: {response})")
