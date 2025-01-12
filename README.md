# Ff2Cql

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/assets/nifi_cassandra.png?raw=true)

A simple transfer data from NiFi FlowFile to CQL (support Apache Cassandra, 
ScyllaDB, AstraDB). The implementation details:
 - development as console application and NiFi v2 processor/extension (support Java 17 and 21)
 - support Apache Cassandra v4/v5, ScyllaDB, AstraDB based on CQL

## 1. The main motivation

 - the Apache NiFi v2 does not support Apache Cassandra v4/v5 (NiFi 2 deprecated 
   support for Cassandra v3)

## 2. Current state
 - 
 - ✅ Console application
   - ✅ Tested with NiFi v2.x
   - ✅ Test in processors **ExecuteProcess** and **ExecuteStreamCommand**
 - ❌ Processor
   - ❌ development in progress

## 3. Usage in NiFi

You can use three options:
 - run console application (Ff2Cql-*.*.jar)
   - with **ExecuteProcess**, where input are CSV files, [see](#31-executeprocess-console-application) 
   - with **ExecuteStreamCommand**, where input is FlowFile/CSV file, [see](#32-executestreamcommand-console-application) 
 - run processor (*.nar) [see](#33-processor)

### 3.1 ExecuteProcess (console application)

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/assets/nifi_executeprocess_2.png?raw=true)

#### Input:
 1. **connection.json** file to CQL, see [chapter 5.2 (Connection setting)](#52-connection-setting)
 2. **CSV file(s) with header** for import (content see [chapter 4.1](#41-expected-contentformat)),
    where the CSV content is based on 'keyspace.table' definition in CQL

#### ExecuteProcess setting (key items):
 - **Command:** 
   - java
 - **Command Argument:**
   - -jar Ff2Cql-1.5.jar import.csv
   - -jar Ff2Cql-1.5.jar import.csv import2.csv
   - -jar Ff2Cql-1.5.jar -c connection-private.json import.csv
   - etc. see [chapter 5.1 (Command line)](#51-command-line)
 - **Working Directory:** 
   - /opt/nifi/nifi-current/bin/test2/
 - **Argument Delimiter:** 
   - ' ' (space)

### 3.2 ExecuteStreamCommand (console application)

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/assets/nifi_executestreamcommand_2.png?raw=true)

#### Input:
 1. **connection.json** file to CQL, see [chapter 5.2 (Connection setting)](#52-connection-setting)
 2. **FlowFile/CSV with header** (content see [chapter 4.1](#41-expected-contentformat)),
    where the FlowFile/CSV content is based on 'keyspace.table' definition in CQL
    (the integration is via stdin)

#### ExecuteStreamCommand setting (key items):
 - **Working Directory:**
   - /opt/nifi/nifi-current/bin/test2/
 - **Command Part:**
   - java
 - **Command Argument Strategy:**
   - Command Arguments Property
 - **Command Arguments:**
   - -jar Ff2Cql-1.5.jar -s
   - -jar Ff2Cql-1.5.jar -c connection-private.json -s
   - etc. see [chapter 5.1 (Command line)](#51-command-line)
 - **Argument Delimiter:**
   - ' ' (space)
 - **Ignore STDIN:**
   - false

### 3.3 Processor
TBD.

## 4. Supported conversions

The solution supports conversion from String to these CQL types:
 - Boolean, TinyInt, SmallInt, Int, BigInt, Float, Double
 - Date, Time, TimeStamp 
 - TimeUUID, UUID
 - Text, varchar (by default)

### 4.1 Expected content/format

The content is CSV with header and comma delimiter and will be use in FlowFile/CSV
or directly in CSV file(s).

#### Sample of CSV file:
```csv
"colbigint","colint","coltext","colfloat","coldouble","coldate","coltime","coltimestamp","colboolean","coluuid","colsmallint","coltinyint","coltimeuuid","colvarchar"
"0","1064","zeVOKGnORq","627.6811","395.8522407512559","1971-11-12","03:37:15","2000-09-25T22:18:45Z","false","6080071f-4dd1-4ea5-b711-9ad0716e242a","8966","55","f45e58f5-c3b7-11ef-8d19-97ae87be7c54","Tzxsw"
"1","1709","7By0z5QEXh","652.03955","326.9081263857284","2013-12-17","08:43:09","2010-04-27T07:02:27Z","false","7d511666-2f81-41c4-9d5c-a5fa87f7d1c3","24399","38","f45e8006-c3b7-11ef-8d19-172ff8d0d752","exAbN"
"2","6249","UYI6AgkcBt","939.01556","373.48559413289485","1980-11-05","15:44:43","2023-11-24T05:59:12Z","false","dbd35d1b-38d0-49a4-8069-9efd68314dc5","6918","72","f45e8007-c3b7-11ef-8d19-d784fa8af8e3","IjnDb"
"3","6998","lXQ69C5HOZ","715.1224","236.7994939033784","1992-02-01","08:07:34","1998-04-09T23:19:18Z","true","84a7395c-94fd-43f5-84c6-4152f0407e93","22123","39","f45e8008-c3b7-11ef-8d19-0376318d55df","jyZo8"
...
```

### 4.2 Format details for CSV:
  - **DATE** 
    - ISO_LOCAL_DATE (format "yyyy-MM-dd"), example '2013-12-17'
  - **TIME**
    - ISO_LOCAL_TIME (format "HH:mm:ss"), example '08:43:09'
  - **TIMESTAMP**
    - ISO 8601 (format "yyyy-MM-dd'T'HH:mm:ss'Z'"), example '2001-01-01T00:00:00Z'

## 5. Others

### 5.1 Command line

The commnad line description:
```
java -jar Ff2Cql-1.5.jar -h
```
Typical output:
```
Usage: example [-dehsV] [-b=<bulk>] [-c=<config>] [INPUT...]
Simple transfer data from NiFi FlowFile to CQL.
      [INPUT...]          Input file(s) for processing (optional in case '-s').
  -b, --bulk=<bulk>       Bulk size for mass upserts (default is 200).
  -c, --config=<config>   Config file (default is 'connection.json').
  -d, --dryRun            Dry run, whole processing without write to CQL.
  -e, --errorStop         Stop processing in case an error.
  -h, --help              Show this help message and exit.
  -s, --stdIn             Use input from stdin (without 'INPUT' file(s)).
  -V, --version           Print version information and exit.
```

### 5.2 Connection setting to CQL

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
 - you can use for base64 e.g. https://www.base64encode.org/
