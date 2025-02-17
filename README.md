# Ff2Cql

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/docs/assets/nifi_cassandra.png?raw=true)

A simple transfer data from NiFi to CQL (support Apache Cassandra, 
ScyllaDB, AstraDB, etc.). The implementation details:
 - development NiFi v2 processor (with controller) and java application (support Java 17/21+)
 - support Apache Cassandra v4/v5, ScyllaDB, AstraDB based on CQL (Cassandra Query Language)

## 1. The main motivation

 - the Apache NiFi v2 does not support Apache Cassandra v4/v5 (NiFi v2 removed 
   the Cassandra processor due to security vulnerabilities and unmaintained 
   code. The processor supported only Cassandra v3 not newer.)

## 2. Usage in NiFi

You can use this preferred way:
 - ✅ **PutCQL** as NiFi processor with controller ([download latest version](./nifi/cql-processor/output/), see 'nifi-cql-nar-*.nar'), where inputs are FlowFiles ([addition detail](./nifi/cql-processor/docs/README.md))
 
or two other alternative ways:
 - ✅ **ExecuteProcess** with java application (see 'Ff2Cql-*.jar'), where inputs are CSV files ([addition detail](./console_app/Ff2Cql/docs/README.md#2-executeprocess-java-application)) 
 - ✅ **ExecuteStreamCommand** with java application (see 'Ff2Cql-*.jar'), where inputs are FlowFiles via stdin ([addition detail](./console_app/Ff2Cql/docs/README.md#3-executestreamcommand-java-application))

