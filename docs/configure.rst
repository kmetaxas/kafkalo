=========
Configure
=========

configuration yaml
------------------

You need to provide a `config.yaml` file so that `kafkalo` will know how to connect and authenticate with your Kafka infrastrucure and related components (Schema registry, RBAC metadata server).

Look at sample config file included.


You can add input dirs with glob patterns to let kafkalo know where to find your YAML definitions. 
Kafkalo will read all the input YAMLs, merge then into a single internal data structure and try to sync them.

input yaml
----------

Kafkalo will read YAML input file and apply the definitions to the Kafka brokers, Schema registry and Metadata service (Confluent RBAC).

A sample YAML file is as follows:


.. code-block:: YAML

   topics:
     - name: SKATA.VROMIA.POLY
       partitions: 6
       replication_factor: 1
       # Any topic configs can be added to this key
       configs:
         cleanup.policy: delete
         min.insync.replicas: 1
         retention.ms: 10000000
       key:
         # Lookup is relative to file
         schema: "schema-key.json"
         compatibility: BACKWARD
       value:
         schema: "schema.json"
         compatibility: NONE
     - name: SKATA.VROMIA.LIGO
       partitions: 6
       replication_factor: 3
       configs:
         cleanup.policy: delete
         min.insync.replicas: 1
       key:
         schema: "schema-key.json"
     - name: SKATA1
       partitions: 1
       replication_factor: 1
     - name: SKATA2
       partitions: 1
       replication_factor: 1
     - name: SKATA3
       partitions: 1
       replication_factor: 1
     - name: SKATA4
       partitions: 1
       replication_factor: 1
     - name: SKATA5
       partitions: 1
       replication_factor: 1
     - name: SKATA6
       partitions: 1
       replication_factor: 1
     - name: SKATA7
       partitions: 1
       replication_factor: 1
   # Clients configures the RBAC (Confluent MDS)
   clients:
     # principals must be in the form User:name or Group:name
     # For each principal you can have a consumer_for, producer_for or resourceowner_for
     # and the topics for each of these categories
     - principal: User:poutanaola
       consumer_for:
         # By default we will use PREFIXED. 
         # set prefixed: false to set it to LITERAL
         - topic: TOPIC1.
         - topic: TOPIC2.
           prefixed: false
       producer_for:
         - topic: TOPIC1.
       resourceowner_for:
         - topic: TOPIC4.
     - principal: Group:malakes
       consumer_for:
         - topic: TOPIC1.
         - topic: TOPIC2.
       producer_for:
         - topic: TOPIC1.
     - principal: User:produser
       producer_for:
         - topic: TOPIC1.
           # Strict mode is mean for production.
           # It will make the producer able to write the topics but read-only
           # access to the schema registry
           strict: false
      # Alllow this principal access to the following consumer groups.
      # roles can be defined but defaults to DeveloperRead
       groups:
         - name: consumer-produser-
         - name: consumer-produser-owner-
           # if not specified, roles is [DeveloperRead]
           roles: ["ResourceOwner"]
           # prefixed is true by default but can be disabled like below
           prefixed: false
     
