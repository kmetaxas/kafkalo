from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry.error import SchemaRegistryError


class MockSRClient(object):
    """
    Mock Schema registry client
    """

    def __init__(self, sr_config):
        self.compatibility = "BACKWARD"
        # {1 : schema} # id:schema
        self.sr_config = sr_config
        self.schemas = {}
        # {'subject_name': {
        #   'versions': {1:Schema_id,2:schema_id},
        #   'compatibility': None
        #  }
        self.subjects = {}

    def get_subjects(self):
        print("Returning subjects: {}".format(self.subjects.keys()))
        return self.subjects.keys()

    def lookup_schema(self, subject_name, schema):
        if subject_name in self.subjects:
            for version, schema_id in self.subjects[subject_name]["versions"].items():
                existing_schema = self.schemas[schema_id]
                if schema == existing_schema:
                    return RegisteredSchema(
                        schema_id=schema_id,
                        schema=self.schemas[schema_id],
                        subject=subject_name,
                        version=version,
                    )
        raise SchemaRegistryError(
            error_code=40403, error_message="Schema not found", http_status_code=404
        )

    def get_or_add_schema(self, schema):
        for schema_id, existing_schema in self.schemas.items():
            if existing_schema == schema:
                return (schema_id, schema)
        if len(self.schemas.keys()):
            new_id = sorted(list(self.schemas.keys()))[-1] + 1
        else:
            new_id = 1
        self.schemas[new_id] = schema
        return (new_id, schema)

    def register_schema(self, subject_name, schema):
        """
        Registered a schema
        """
        print(f"Registerin schema {subject_name} {schema}")
        schema_id, existng_schema = self.get_or_add_schema(schema)
        if subject_name in self.subjects:
            # IT already exist so register a new version

            new_version = (
                sorted(list(self.subjects[subject_name]["versions"].keys()))[-1] + 1
            )
            self.subjects[subject_name]["versions"][new_version] = schema_id
        else:
            # New schema
            self.subjects[subject_name] = {
                "versions": {1: schema_id},
            }
        print(f"Returning id: {schema_id}")
        return schema_id

    def get_compatibility(self, subject_name=None):
        if not subject_name:
            return {"compatibilityLevel": self.compatibility}
        if subject_name in self.subjects:
            if "compatibility" in self.subjects[subject_name]:
                return {
                    "compatibilityLevel": self.subjects[subject_name]["compatibility"]
                }
        raise SchemaRegistryError(
            error_code=40401, error_message="Subject not found", http_status_code=404
        )

    def set_compatibility(self, subject_name=None, level=None):
        if not subject_name and not level:
            raise SchemaRegistryError("No subject or level set")
        if not subject_name:
            self.compatibility = level
            return
        if not level:
            level = self.compatibility
        if subject_name not in self.subjects:
            raise SchemaRegistryError(
                error_code=40401,
                error_message="Subject not found",
                http_status_code=404,
            )
        self.subjects[subject_name].update({"compatibility": level})
