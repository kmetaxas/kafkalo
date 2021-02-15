from typing import List
import requests
import json
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

    # Context names
    CTX_KAFKA = 1  # KAfka
    CTX_SR = 2  # Schama registry
    CTX_KSQL = 3  # KSQLDB
    CTX_CONNECT = 4  # Connect

    def __init__(self, mds_config):
        self.mds_config = mds_config
        self.url = mds_config["url"]
        self.auth = (mds_config["username"], mds_config["password"])
        self.kafka_cluster_id = self.get_kafka_cluster_id()
        self.schema_registry_cluster_id = mds_config.get(
            "schema-registry-cluster-id", None
        )
        self.connect_cluster_id = mds_config.get("connect-cluster-id", None)
        self.ksql_cluster_id = mds_config.get("ksql-cluster-id", None)

    def get_kafka_cluster_id(self):
        r = requests.get(self.url + "/security/1.0/metadataClusterId", auth=self.auth)
        return r.text

    def do_consumer_for(self, topic, principal, prefixed=True):
        """
        Convenience method that assigns a set of permisions for a typical
        reader client
        """
        context = self._get_context(MDSAdmin.CTX_KAFKA)
        patternType = "PREFIXED"
        if not prefixed:
            patternType = "LITERAL"
        # TODO do resourcePatterns list for all topics listed if possible
        data = {
            "scope": context,
            "resourcePatterns": [
                {
                    "resourceType": "Topic",
                    "name": topic,
                    "patternType": patternType,
                }
            ],
        }
        consumer_roles = ["DeveloperRead"]
        for roleName in consumer_roles:
            try:
                r = requests.post(
                    self.url
                    + f"/security/1.0/principals/User:{principal}/roles/{roleName}/bindings",
                    auth=self.auth,
                    json=data,
                )
                print("RESP=" + r.json())
                r.raise_for_status()
            except:
                pass

    def do_producer_for(self, topic, principal, prefixed=True):
        """
        Convenience method that assigns a set of permisions for a typical
        producer client
        """
        pass

    def do_resourceowner_for(self, topic, principal, prefixed=True):
        """
        Convenience method that assigns a set of permissions for a
        resourceowner. (read/write AND delegate)
        """

    def _get_context(self, ctx):
        """
        Generate a context to be used as param for MDS API
        """
        context = {
            "clusters": {
                "kafka-cluster": self.kafka_cluster_id,
            },
        }
        if ctx == MDSAdmin.CTX_KAFKA:
            return context
        if ctx == MDSAdmin.CTX_SR:
            context["kafka-schema-registry-cluster"] = self.schema_registry_cluster_id
            return context
        if ctx == MDSAdmin.CTX_KSQL:
            context["ksql-cluster"] = self.ksql_cluster_id
            return context
        if ctx == MDSAdmin.CTX_CONNECT:
            context["connect-cluster"] = self.connectl_cluster_id
            return context

    def get_rolebinding_for_user(self, username):
        data = self._get_context(MDSAdmin.CTX_KAFKA)
        r = requests.post(
            self.url + f"/security/1.0/lookup/principals/User:{username}/roleNames",
            auth=self.auth,
            json=data,
        )
        result = r.json()
        print(f"rolebinding list: {result}")
        return r.json()
