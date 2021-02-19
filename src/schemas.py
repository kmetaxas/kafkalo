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
        self.subject_cache = []
        # plan is a dict with the following keys:
        # 'subject' (name), 'schema': Schema obj, 'status': one of 'new' or
        # 'updated'. 'compatibility': {"old":str, "new":str}
        self.dry_run_plan = {}
        self._populate_subject_cache()

    def _populate_subject_cache(self):
        """
        List all subjects and populate the subject name cache
        """
        self.subject_cache = self.client.get_subjects()
        return self.subject_cache

    def get_dry_run_plan(self):
        return self.dry_run_plan

    def _add_to_plan(self, schema, compatibility=None):
        """
        Add schema to plan.
        If compatibility is None it means we are adding a new schema and don't
        have compatibility info yet.
        """
        if (not compatibility) and schema.subject_name in self.dry_run_plan:
            # if we are adding a new schema (no compatibility) then make sure
            # it does not already exist
            raise Exception(f"Plan {schema.subject_name} already exists in plan!")
        # Does it already exist in schema registry? set status field
        if schema.subject_name not in self.subject_cache:
            status = "new"
        else:
            status = "update"

        self.dry_run_plan[schema.subject_name] = {
            "schema": schema,
            "status": "new",
            "compatibility": {},
        }

    def reconcile_schemas(self, schemas: List[Schema], dry_run=False):
        """
        Iterate of the provided schemas and ensure they are as specified
        :dry_run don't change anything but display what would happen
        """
        missing_schemas = []
        found_schemas = []
        for schema in schemas:
            # Does it exist (with same schema content?)
            try:
                found_schema = self.client.lookup_schema(
                    schema.subject_name, schema.schema
                )
                found_schemas.append(found_schema)
            except SchemaRegistryError as e:
                missing_schemas.append(schema)
            # TODO does it also have the requested compatibility mode? Don't
            # miss updating the compatibility

        print(f"Found schemas {found_schemas} but we are missing {missing_schemas}")

        # Create missing schemas
        failed_to_register = {}
        registered = []
        for schema in missing_schemas:
            # Register schema
            try:
                if not dry_run:
                    self.client.register_schema(schema.subject_name, schema.schema)
                    registered.append(schema)
                else:
                    self._add_to_plan(schema)
            except SchemaRegistryError as e:
                failed_to_register[schema.subject_name] = {
                    "schema": schema,
                    "reason": str(e),
                }
            # Set compatibility of specified
            if schema.compatibility and schema.subject_name in self.subject_cache:
                compat = schema.compatibility.lower().strip()
                # First get the compatiblity
                current_compat = self.client.get_compatibility(schema.subject_name)
                if current_compat != compat:
                    try:
                        if not dry_run:
                            self.client.set_compatibility(
                                schema.subject_name, level=compat
                            )
                        else:
                            self._add_to_plan(
                                schema,
                                compatibility={"old": current_compat, "new": compat},
                            )
                    except SchemaRegistryError as e:
                        print(
                            f"Failed to set compatibility to '{schema.compatibility} for {schema.subject_name} with error: {e}"
                        )

        print(f"Registered: {registered} and failed: {failed_to_register}")
