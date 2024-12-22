# Csv2Cql

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/assets/nifi_cassandra.png?raw=true)

A simple transfer data from CSV/FileFlow to CQL. The implementation details:
 - development as console application and NiFi processor-extension (in Java 17 and 21)
 - support Apache Cassandra v4/v5, ScyllaDB, AstraDB based on CQL

### The main motivations:
 - support integration between Apache NiFi and Apache Cassandra
 - the Apache NiFi 2 does not suppport Apache Cassandra v4/v5

### Current state:
 - under development (unstable)
