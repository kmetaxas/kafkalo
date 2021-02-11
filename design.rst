Design
------

Features

- Read YAML input
- Create topics
  - accept all properties of Kafka topics (+arbitrary so that we don't have to be explicit in what we support but passthrough to kafka adminclient)
- Assign permissions:
  - Specify resource name (choose PREFIX or LITERAL)
  - Consumer (RO or RW)
     - To topics
     - To consumergroup
     - To Schema registry subhects
  - Producers
     - To topics (Write)
     - To Schema registry subhects
       - Specify Readonly or RW
- Create schemas
   - Assign schema to topic if provided.
- Produce PLAN (dry run with steps).
- Optionally Produce equivalent shell script file to make changes outside app
- Optional - Handle deletions
- Optional - monitor resources and enforce/warn on non-compliance (resources managed outside of app)
- Optional - Manage users in FreeIPA


Required inputs
===============

- Schema registry URL
- Kafka broker
- MDS API
