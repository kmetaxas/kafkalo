from typing import List
from confluent_kafka.schema_registry import Schema as CPSchema
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
        if compatibility:
            self.compatibility = compatibility.strip().lower()
        else:
            self.compatibility = None


class SchemaAdmin(object):
    """
    Manage schemas
    """

    def __init__(self, sr_client):
        self.client = sr_client
        self.subject_cache = []
        # plan is a dict with the following keys:
        # 'subject' (name), 'schema': Schema obj, 'status': one of 'new' or
        # 'updated'. 'compatibility': {"old":str, "new":str}
        self.dry_run_plan = {}
        self._populate_subject_cache()
        self.global_compat = self._get_global_compat()

    def _get_global_compat(self):
        """
        Geth the global compatibility setting of the schema registry
        """
        global_compat = self.client.get_compatibility()["compatibilityLevel"].lower()

    def _populate_subject_cache(self):
        """
        List all subjects and populate the subject name cache
        """
        self.subject_cache = self.client.get_subjects()
        return self.subject_cache

    def get_dry_run_plan(self):
        return self.dry_run_plan

    def _add_to_plan(self, schema, data=None):
        """
        Add schema to plan.
        """
        if schema.subject_name not in self.dry_run_plan:
            self.dry_run_plan[schema.subject_name] = {
                "schema": schema,
                "status": "created",
            }
        if data:
            self.dry_run_plan[schema.subject_name].update(data)

    def lookup_schema(self, schema):
        """
        Wrappper around SchemaRegistryClient.lookup_schema that accepts Schema
        object.
        Returns tuple (True, RegisteredSchema) if success or (False, Error)
        otherwise
        """
        try:
            registered = self.client.lookup_schema(schema.subject_name, schema.schema)
            return (True, registered)
        except SchemaRegistryError as e:
            return (False, e)

    def get_subjects_to_update(self, schemas: List[Schema]):
        """
        Return a list of schemas that need updating.
        Lookup the schemas in the schema registry and if they already exist,
        exclude them
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
            except SchemaRegistryError:
                # TODO make sure its a 404 and not some other error

                missing_schemas.append(schema)
        return missing_schemas

    def update_or_create_schema(self, schema: Schema, dry_run=False):
        """
        Register or create a schema
        """
        created = error = None
        try:
            if not dry_run:
                self.client.register_schema(schema.subject_name, schema.schema)
                created = schema
            else:
                status = "created"
                if schema.subject_name in self.subject_cache:
                    status = "updated"
                self._add_to_plan(schema, {"status": status})

        except SchemaRegistryError as e:
            error = {
                schema.subject_name: {
                    "schema": schema,
                    "reason": str(e),
                }
            }
            print(f"Error registering schema for {schema.subject_name}: {e}")
            self._add_to_plan(schema, {"status": "failed"})

        return (created, error)

    def reconcile_schemas(self, schemas: List[Schema], dry_run=False):
        """
        Iterate of the provided schemas and ensure they are as specified
        :dry_run don't change anything but display what would happen
        """

        update_subjects = self.get_subjects_to_update(schemas)

        # Create missing schemas
        failed_to_register = {}
        registered = []
        for schema in update_subjects:
            # Set compatibility before registering to ensure we don't get
            # compatibility errors if compat has changed.
            self.set_compatibility(schema, dry_run=dry_run)
            # Register schema
            created, error = self.update_or_create_schema(schema, dry_run=dry_run)
            if created:
                registered.append(created)
            if error:
                failed_to_register.update({schema.subject_name: error})
            self.set_compatibility(schema, dry_run=dry_run)

    def set_compatibility(self, schema: Schema, dry_run=False):
        """
        Set compatibility level for a Schema, if needed
        """
        global_compat = self.global_compat
        # per_subject_override_exists = False
        # Set compatibility of specified
        if schema.compatibility and schema.subject_name in self.subject_cache:
            compat = schema.compatibility
            # First get the compatiblity
            try:
                current_compat = self.client.get_compatibility(schema.subject_name)[
                    "compatibilityLevel"
                ].lower()
                # per_subject_override_exists = True
            except SchemaRegistryError as e:
                print(f"Got error {e}")
                current_compat = global_compat

            if current_compat != compat:
                print(
                    f"Will update compat for {schema.subject_name} from {current_compat} to {compat}"  # noqa: E501
                )
                try:
                    if not dry_run:
                        self.client.set_compatibility(schema.subject_name, level=compat)
                    else:
                        print("compat differ. add to plan")
                        self._add_to_plan(
                            schema,
                            {"compatiblity": {"old": current_compat, "new": compat}},
                        )
                except SchemaRegistryError as e:
                    print(
                        f"Failed to set compatibility to '{schema.compatibility} for {schema.subject_name} with error: {e}"  # noqa: E501
                    )
