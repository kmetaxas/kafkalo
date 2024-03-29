Kafkalo - A Tool to manage Confluent Kafka
==========================================

** STATUS: This project has been replaced by Gafkalo_ **
It will be archived. 

Kafkalo is a tool to manage Kafka and Confluent Platform.


It manages:

- Topic creation and changes of settings
- Schema creation and versioning.
- Confluent RBAC rolebindings with predefined roles.

It also has a dry-run function that generates a plan. Whenever possible it will contact the services (for example Kafka broker with validate_only=True)
and provide feedback on errors that will happen.

You provide a list of YAML input files (for example in a directory, with a glob pattern) and then Kafkalo will try to apply whatever is defined.


Usage
-----

You can provide a `config.yaml` file so that `kafkalo` will know how to connect and possibly authenticate with your Kafka infrastrucure and related components (Schema registry, RBAC metadata server).

Look at sample config file included.

You can add input dirs with glob patterns to let kafkalo know where to find your YAML definitions. 
Kafkalo will read all the input YAMLs, merge then into a single internal data structure and try to sync them.

To generate a plan

> kafkalo plan

Once you are satisfied with the plan you can let `kafkalo` sync:

> kafkalo sync


.. _Gafkalo: https://github.com/kmetaxas/gafkalo/
