from typing import List
import requests
from requests.auth import HTTPBasicAuth


class Client(object):
    """
    Represents a single client (and therefore User principal)
    """

    def __init__(
        self, principal, consumer_for=None, producer_for=None, resourceowner_for=None
    ):
        self.consumer_for = consumer_for
        self.producer_for = producer_for
        self.resourceowner_for = resourceowner_for


class MDSAdmin(object):
    """
    Manage schema registry
    """

    def __init__(self, mds_config):
        self.mds_config = mds_config
        self.url = mds_config["url"]
        self.auth = (mds_config["username"], mds_config["password"])
        self.kafka_cluster_id = self.get_kafka_cluster_id()
        self.schema_registry_cluster_id = mds_config.get(
            "schema-registry-cluster-id", None
        )

    def get_kafka_cluster_id(self):
        r = requests.get(self.url + "/security/1.0/metadataClusterId", auth=self.auth)
        return r.text

    def get_rolebinding_for_user(self, username):
        # TODO iterate cluster names getting roles for all cluster ids
        cluster_names = {
            "schema-registry-cluster": self.schema_registry_cluster_id,
            "connect-cluster": self.connect_cluster_id,
            "ksql-cluster": self.connect_cluster_id,
        }
        data = {
            "clusters": {
                "kafka-cluster": self.kafka_cluster_id,
            },
        }
        r = requests.post(
            self.url + f"/security/1.0/lookup/principals/User:{username}/roleNames",
            auth=self.auth,
            json=data,
        )
        result = r.json()
        print(f"rolebinding list: {result}")
        return r.json()
