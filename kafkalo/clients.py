from typing import List
import requests


class Client(object):
    """
    Represents a single client (and therefore User principal)
    """

    def __init__(
        self,
        principal,
        consumer_for=None,
        producer_for=None,
        resourceowner_for=None,
        groups=None,
    ):
        if ":" not in principal or principal.split(":")[0] not in ["User", "Group"]:
            raise Exception("Principal {principal} not in the form User/Group:<name>")

        self.principal = principal
        self.consumer_for = consumer_for
        self.producer_for = producer_for
        self.resourceowner_for = resourceowner_for
        self.groups = groups


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
        # If dry_run is enabld then each method that actually modifies
        # resources will record its plan in this data structure. We can then
        # use this to present a nice plan to the user (for example with a
        # Jinja2 template)
        self.dry_run_plan = {"rolebindings": []}
        self.resource_types = ("Topic", "Group", "Cluster", "Subject")

    def get_dry_run_plan(self):
        return self.dry_run_plan

    def get_kafka_cluster_id(self):
        r = requests.get(self.url + "/security/1.0/metadataClusterId", auth=self.auth)
        return r.text

    def _set_rolebinding(
        self,
        ctx,
        resource_type: str,
        resource_name: str,
        principal: str,
        roles: list,
        prefixed=True,
        dry_run=False,
    ):
        """
        Set the rolebindings listed in rolebindings for the given principal and
        resource type.
        :ctx context (Kafka, schema registry etc. as defined in MDSAdmin.CTX_*)
        :resource_type Topic,Group,Cluster etc
        :resource_name the name of the resource. (name of topic or subject)
        :principal the principal name.
        :roles a list of role nmes
        :prefixed Use prefixed rolebinding (defaults to true)
        :dry_run don't change but record in dry_run_plan
        """

        context = self._get_context(ctx)
        patternType = "PREFIXED"
        if not prefixed:
            patternType = "LITERAL"
        data = {
            "scope": context,
            "resourcePatterns": [
                {
                    "resourceType": resource_type,
                    "name": resource_name,
                    "patternType": patternType,
                }
            ],
        }
        for roleName in roles:
            if not dry_run:
                try:
                    r = requests.post(
                        self.url
                        + f"/security/1.0/principals/{principal}/roles/{roleName}/bindings",  # noqa: E501
                        auth=self.auth,
                        json=data,
                    )
                    r.raise_for_status()
                except Exception as e:
                    print(
                        f"Failed to set RBAC {roleName} for {principal} with error {e.text}"  # noqa: E501
                    )
            else:
                data["principal"] = principal
                data["role"] = roleName
                # TODO either don't add if already exists or add metadata so
                # that we can only display rolebindings to be created only
                self.dry_run_plan["rolebindings"].append(data.copy())

    def do_consumer_for(self, topic, principal, prefixed=True, dry_run=False):
        """
        Convenience method that assigns a set of permisions for a typical
        reader client
        """
        consumer_roles = ["DeveloperRead"]
        self._set_rolebinding(
            MDSAdmin.CTX_KAFKA,
            "Topic",
            topic,
            principal,
            consumer_roles,
            prefixed,
            dry_run=dry_run,
        )
        # add schema registry roles
        self._set_rolebinding(
            MDSAdmin.CTX_SR,
            "Subject",
            topic,
            principal,
            consumer_roles,
            prefixed,
            dry_run=dry_run,
        )

    def do_producer_for(
        self, topic, principal, prefixed=True, strict=False, dry_run=False
    ):
        """
        Convenience method that assigns a set of permisions for a typical
        producer client
        :strict strict mode meant for production environments. give write to topic but only
        read on schema registry
        """
        roles = ["DeveloperWrite"]
        self._set_rolebinding(
            MDSAdmin.CTX_KAFKA,
            "Topic",
            topic,
            principal,
            roles,
            prefixed,
            dry_run=dry_run,
        )
        sr_roles = ["DeveloperRead"]
        # If stict mode is set, don't allow develerWrite on the schema
        # registry.
        if not strict:
            sr_roles.append("DeveloperWrite")
        self._set_rolebinding(
            MDSAdmin.CTX_SR,
            "Subject",
            topic,
            principal,
            sr_roles,
            prefixed,
            dry_run=dry_run,
        )

    def do_resourceowner_for(self, topic, principal, prefixed=True, dry_run=False):
        """
        Convenience method that assigns a set of permissions for a
        resourceowner. (read/write AND delegate)
        """
        roles = ["ResourceOwner"]
        self._set_rolebinding(
            MDSAdmin.CTX_KAFKA,
            "Topic",
            topic,
            principal,
            roles,
            prefixed,
            dry_run=dry_run,
        )
        # add schema registry roles
        self._set_rolebinding(
            MDSAdmin.CTX_SR,
            "Subject",
            topic,
            principal,
            roles,
            prefixed,
            dry_run=dry_run,
        )

    def _get_context(self, ctx):
        """
        Generate a context dict suitable to be used as param for MDS API
        :ctx the context type (schema registry, kafka etc) as defined in MDSAdmin.CTX_*
        """
        context = {
            "clusters": {
                "kafka-cluster": self.kafka_cluster_id,
            },
        }
        if ctx == MDSAdmin.CTX_KAFKA:
            return context
        if ctx == MDSAdmin.CTX_SR:
            context["clusters"][
                "schema-registry-cluster"
            ] = self.schema_registry_cluster_id
            return context
        if ctx == MDSAdmin.CTX_KSQL:
            context["clusters"]["ksql-cluster"] = self.ksql_cluster_id
            return context
        if ctx == MDSAdmin.CTX_CONNECT:
            context["clusters"]["connect-cluster"] = self.connectl_cluster_id
            return context

    def get_rolebinding_for_user(self, username):
        data = self._get_context(MDSAdmin.CTX_KAFKA)
        r = requests.post(
            self.url + f"/security/1.0/lookup/principals/{username}/roleNames",
            auth=self.auth,
            json=data,
        )
        result = r.json()
        print(f"rolebinding list: {result}")
        return r.json()

    def do_group(self, name, principal, prefixed=True, roles=None, dry_run=False):
        """
        Add consumer group rolebindings
        """
        if not roles:
            roles = ["DeveloperRead"]
        self._set_rolebinding(
            MDSAdmin.CTX_KAFKA,
            "Group",
            name,
            principal,
            roles,
            prefixed=prefixed,
            dry_run=dry_run,
        )

    def reconcile_roles(self, clients: List[Client], dry_run=False):
        """
        Iterate over Client list and reconcile current with desired
        configuration
        :clients a list of Client objects
        :dry_run Don't change anything but display what would change (and
        validate if possible)
        """
        if not clients:
            return
        for client in clients:
            principal = client.principal
            if client.consumer_for:
                for topic in client.consumer_for:
                    self.do_consumer_for(
                        topic=topic["topic"],
                        principal=principal,
                        prefixed=topic.get("prefixed", True),
                        dry_run=dry_run,
                    )
            if client.producer_for:
                for topic in client.producer_for:
                    self.do_producer_for(
                        topic=topic["topic"],
                        principal=principal,
                        prefixed=topic.get("prefixed", True),
                        strict=topic.get("strict", False),
                        dry_run=dry_run,
                    )
            if client.resourceowner_for:
                for topic in client.resourceowner_for:
                    self.do_resourceowner_for(
                        topic=topic["topic"],
                        principal=principal,
                        prefixed=topic.get("prefixed", True),
                        dry_run=dry_run,
                    )
            if client.groups:
                for group in client.groups:
                    self.do_group(
                        name=group["name"],
                        principal=principal,
                        prefixed=group.get("prefixed", True),
                        roles=group.get("roles", ["DeveloperRead"]),
                        dry_run=dry_run,
                    )
