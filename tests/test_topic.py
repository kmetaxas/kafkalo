from kafkalo.topics import KafkaAdmin


def test_get_config_diff():
    """
    Ensure config diff actually produces a diff
    """
    kafka_admin = KafkaAdmin(adminclient=None, consumer=None)
    assert isinstance(kafka_admin, KafkaAdmin)
    old_configs = {"retention.ms": "100", "message.max.bytes": "1337"}
    new_configs = {"retention.ms": "1", "message.max.bytes": "1337"}

    config_delta = kafka_admin._get_config_diff(old_configs, new_configs)
    print(config_delta)
    assert "retention.ms" in config_delta
    assert "message.max.bytes" not in config_delta
    assert config_delta["retention.ms"]["before"] == "100"
    assert config_delta["retention.ms"]["after"] == "1"
