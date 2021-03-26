=====
Usage
=====


To generate a plan

.. code-block:: bash

   kafkalo plan --config myconfig.yaml

This will produce a plan output with the changes that `kafkalo` will make.

(We don't yet check existing RBAC definitions so RBAC is always included in the plan. This should be changed in later versions)

Once you are satisfied with the plan you can let `kafkalo` sync:

.. code-block:: bash

  kafkalo sync --config myconfig.yaml

This command will read all the yaml files specified in the config , merge them and apply them.


schema tools
------------

To help with administration and troubleshooting there is a schema subcommand:

`kafkalo schema check-exists` will check if a provided schema file is registered under a specific subject name. For example:

.. code-block:: bash

   $ kafkalo schema check-exists --subject SKATA.VROMIA.POLY-value  --schema-file data/schema.json --config config.yaml


will tell you if schema.json is registered under `SKATA.VROMIA.POLY-value` and, if so, what is the `version` and `id`.


