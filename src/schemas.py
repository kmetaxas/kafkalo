from confluent_kafka import KafkaException
from typing import List
from confluent_kafka.schema_registry import Schema as CPSchema
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError


class Schema(object):
    """
    Represents a single Schema.
    """

    def __init__(self, subject_name: str, schema: str, compatibility=None):
        """
        A Schema object
        :subject_name the schema subject
        :schema the schema representation
        :compatibility the compatibility level. Set to python None object will
        use the Schema registry default
        """

        self.subject_name = subject_name
        self.schema_json = schema
        # TODO AVRO hardcoded. Fix this by adding a field to yaml and default
        # to AVRO
        self.schema = CPSchema(self.schema_json, "AVRO")
        self.compatibility = compatibility


class SchemaAdmin(object):
    """
    Manage schemas
    """

    def __init__(self, sr_config):
        self.client = SchemaRegistryClient(sr_config)

    def reconcile_schemas(self, schemas: List[Schema], dry_run=False):
        """
        Iterate of the provided schemas and ensure they are as specified
        :dry_run don't change anything but display what would happen
        """
        missing_schemas = []
        found_schemas = []
        for schema in schemas:
            # Does it exist
            try:
                found_schema = self.client.lookup_schema(
                    schema.subject_name, schema.schema
                )
                found_schemas.append(found_schema)
            except SchemaRegistryError as e:
                missing_schemas.append(schema)

        print(f"Found schemas {found_schemas} but we are missing {missing_schemas}")

        # Create missing schemas
        failed_to_register = {}
        registered = []
        for schema in missing_schemas:
            # Register schema
            try:
                self.client.register_schema(schema.subject_name, schema.schema)
                registered.append(schema)
            except SchemaRegistryError as e:
                failed_to_register[schema.subject_name] = {
                    "schema": schema,
                    "reason": str(e),
                }
            # Set compatibility of specified
            if schema.compatibility:
                try:
                    self.client.set_compatibility(
                        schema.subject_name, level=schema.compatibility
                    )
                except SchemaRegistryError as e:
                    print(
                        f"Failed to set compatibility to '{schema.compatibility} for {schema.subject_name} with error: {e}"
                    )

        print(f"Registered: {registered} and failed: {failed_to_register}")
