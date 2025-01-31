## 1. Usage in NiFi

You can use java application (Ff2Cql-*.jar) directly in NiFi:
- ✅ **ExecuteProcess**, where input are CSV files, [see](#2-executeprocess-java-application)
- ✅ **ExecuteStreamCommand**, where input are FlowFiles, [see](#3-executestreamcommand-java-application)

## 2. ExecuteProcess (java application)

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/console_app/Ff2Cql/docs/assets/nifi_executeprocess_2.png?raw=true)

### Input
- **connection.json** file to CQL, [see](#42-connection-setting-to-cql)
- **CSV file(s) with header** for import (content [see](../../../README.md#31-expected-contentformat)),
   where the CSV content is based on 'keyspace.table' definition in CQL

### ExecuteProcess setting (key items)
- **Command:**
    - java
- **Command Argument:**
    - -jar Ff2Cql-1.7.jar import.csv
    - -jar Ff2Cql-1.7.jar import.csv import2.csv
    - -jar Ff2Cql-1.7.jar -c connection-private.json import.csv
    - etc. [see](#41-command-line)
- **Working Directory:**
    - /opt/nifi/nifi-current/bin/test2/
- **Argument Delimiter:**
    - ' ' (space)

## 3. ExecuteStreamCommand (java application)

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/console_app/Ff2Cql/docs/assets/nifi_executestreamcommand_2.png?raw=true)

#### Input
- **connection.json** file to CQL, [see](#42-connection-setting-to-cql)
- **FlowFile/CSV with header** (content [see](../../../README.md#31-expected-contentformat)),
   where the FlowFile/CSV content is based on 'keyspace.table' definition in CQL
   (the integration is via stdin)

### ExecuteStreamCommand setting (key items)
- **Working Directory:**
    - /opt/nifi/nifi-current/bin/test2/
- **Command Path:**
    - java
- **Command Argument Strategy:**
    - Command Arguments Property
- **Command Arguments:**
    - -jar Ff2Cql-1.7.jar -s
    - -jar Ff2Cql-1.7.jar -c connection-private.json -s
    - etc. [see](#41-command-line)
- **Argument Delimiter:**
    - ' ' (space)
- **Ignore STDIN:**
    - false

## 4. Others

## 4.1 Command line

The command line description:
```
java -jar Ff2Cql-1.7.jar -h
```
Typical output:
```
Usage: example [-dehsV] [-b=<bulk>] [-c=<config>] [INPUT...]
Simple transfer data from NiFi FlowFile to CQL.
      [INPUT...]          Input file(s) for processing (optional in case '-s').
  -b, --batch=<batch>     Batch size for mass upserts (default is 200).
  -c, --config=<config>   Config file (default is 'connection.json').
  -d, --dryRun            Dry run, whole processing without write to CQL.
  -e, --errorStop         Stop processing in case an error.
  -h, --help              Show this help message and exit.
  -s, --stdIn             Use input from stdin (without 'INPUT' file(s)).
  -V, --version           Print version information and exit.
```

## 4.2 Connection setting to CQL

The default template with description:
```
{
  "ipAddresses": ["<add ip>","<add ip2>","..."],
  "port": 9042,
  "username": "<add username>",
  "pwd": "<add pwd in base64>",
  "localDC": "<local data center>",
  "connectionTimeout": "<timeout in seconds>",
  "requestTimeout": "<timeout in seconds>",
  "consistencyLevel": "<consistency level e.g. LOCAL_ONE, LOCAL_QUORUM, etc.>",
  "table": "<add keyspace.table in CQL>"
}
```
The real config content:
```
{
  "ipAddresses": ["10.129.53.10","10.129.53.11","10.129.53.12"],
  "port": 9042,
  "username": "app_w4c",
  "pwd": "bXkgcGFzc3dvcmQ=",
  "localDC": "dc1",
  "connectionTimeout": "900",
  "requestTimeout": "60",
  "consistencyLevel": "LOCAL_ONE",
  "table": "app_w4c_main.dynamic_party"
}
```
NOTE:
- you can use for base64 e.g. https://www.base64encode.org/ or https://www.base64decode.org/
