from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


class ConfigError(Exception):
    """
    Configuration Exception
    """

    pass


class Config(object):
    """
    Read configuration
    """

    def __init__(self, filename):
        if not filename:
            raise Exception("No filename provided for Config object")
        self.config = self._parse_yaml(filename)
        # TODO config logging

    def _parse_yaml(self, filename):
        """
        PArse the actual YAML with config data
        """
        with open(filename, "r") as fp:

            config = load(fp.read(), Loader=Loader)
            self._validate_config(config)
            return config

    def _validate_config(self, config):
        """
        Quick validation of config file
        """
        if not config.get("connections", None):
            raise ConfigError("connections key missing")
        connections = config["connections"]
        if not connections.get("kafka", None):
            raise ConfigError("connections.kafka key missing")
        if not connections.get("schemaregistry", None):
            raise ConfigError("connections.schemaregistry key missing")

    def get_kafka_config(self):
        return self.config["connections"]["kafka"]

    def get_sr_config(self):
        return self.config["connections"]["schemaregistry"]

    def get_mds_config(self):
        return self.config["connections"]["mds"]

    def get_input_patterns(self):
        if "kafkalo" in self.config:
            return self.config["kafkalo"].get("input_dirs", None)
        return None
