# TODO
  * Cache database lookups in Anomaly Detection code (so you don't have to query the database all of the time
  * Run Neo4j CSV and Anomaly Database as separate processes so they don't get in each other's ways
    - I.e., create a common.py where they can get stuff from the kafka server
  * Enable SSL on CDM consumer
  * Create python bindings repo for us internally and fix the issue with avro==1.8.2-tupty
