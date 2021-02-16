from config import Config

SAMPLE_FILE = "tests/data/config.sample.yaml"


def test_config_load():
    config = Config(filename=SAMPLE_FILE)
    assert isinstance(config, Config)


def test_get_kafka_config():
    config = Config(filename=SAMPLE_FILE)
    kafka_config = config.get_kafka_config()
    assert kafka_config["bootstrap.servers"] == "localhost:9093"
    assert kafka_config["group.id"] == "kafkalo_consumer"


def test_get_sr_config():
    config = Config(filename=SAMPLE_FILE)
    kafka_config = config.get_sr_config()
    assert kafka_config["url"] == "http://localhost:8081"
    assert kafka_config["basic.auth.user.info"] == "user:password"


def test_get_mds_config():
    config = Config(filename=SAMPLE_FILE)
    kafka_config = config.get_mds_config()
    assert kafka_config["url"] == "http://localhost:8090"
    assert kafka_config["username"] == "username"
    assert kafka_config["schema-registry-cluster-id"] == "schemaregistry"
