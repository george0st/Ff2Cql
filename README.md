# Csv2Cql

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/assets/nifi_cassandra.png?raw=true)

A simple transfer from CSV to CQL data. The implementation details:
 - development as console application and NiFi extension (in Java 17 and 21)
 - support Apache Cassandra v4/v5, ScyllaDB, AstraDB based on CQL

### The main motivations:
 - support integration between Apache NiFi and Apache Cassandra
 - the Apache NiFi 2 (last version) does not suppprt Apache Cassandra v4 and v5

### Current state:
 - under development (unstable)
