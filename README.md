# FfCsv2Cql

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/assets/nifi_cassandra.png?raw=true)

A simple transfer data from FileFlow/CSV to CQL (support Apache Cassandra, 
ScyllaDB, AstraDB). The implementation details:
 - development as console application and NiFi processor/extension (support Java 17 and 21)
 - support Apache Cassandra v4/v5, ScyllaDB, AstraDB based on CQL

### The main motivations:
 - support integration between Apache NiFi and Apache Cassandra
 - the Apache NiFi 2 does not support Apache Cassandra v4/v5 (NiFi 2 deprecated 
   support for Cassandra v3)

### Current state:

![Work in progress](https://github.com/george0st/Csv2Cql/blob/main/assets/work-in-progress2.png?raw=true)
 - under development (unstable)

### Supported conversions

The solution supports conversion from String to these CQL types:
 - Boolean, TinyInt, SmallInt, Int, BigInt, Float, Double
 - Date, Time, TimeStamp 
 - TimeUUID, UUID
 - Text, varchar (by default)