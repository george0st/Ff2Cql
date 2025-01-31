# Ff2Cql

![NiFi + Cassandra](https://github.com/george0st/Csv2Cql/blob/main/assets/nifi_cassandra.png?raw=true)

A simple transfer data from NiFi FlowFile to CQL (support Apache Cassandra, 
ScyllaDB, AstraDB, etc.). The implementation details:
 - development as java application and NiFi v2 processor (support Java 17 and 21)
 - support Apache Cassandra v4/v5, ScyllaDB, AstraDB based on CQL (Cassandra Query Language)

## 1. The main motivation

 - the Apache NiFi v2 does not support Apache Cassandra v4/v5 (NiFi v2 removed 
   the Cassandra processor due to security vulnerabilities and unmaintained 
   code. The processor supported only Cassandra v3)

## 2. Usage in NiFi

You can use three options:
 - ✅ **PutCQL** as NiFi processor (see 'nifi-cql-nar-*.nar'), where input are FlowFiles, [see](./nifi/cql-processor/docs/README.md) 
 - ✅ **ExecuteProcess** as java application (see 'Ff2Cql-*.jar'), where input are CSV files, [see](./console_app/Ff2Cql/docs/README.md#11-executeprocess-console-application) 
 - ✅ **ExecuteStreamCommand** as java application (see 'Ff2Cql-*.jar'), where input are FlowFiles, [see](./console_app/Ff2Cql/docs/README.md#12-executestreamcommand-console-application)

## 3. Supported conversions

The solution supports conversion from String to these CQL types:
 - Boolean, TinyInt, SmallInt, Int, BigInt, Float, Double
 - Date, Time, TimeStamp 
 - TimeUUID, UUID
 - Text, varchar (by default)

### 3.1 Expected content/format

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

### 3.2 Format details for CSV:
  - **DATE** 
    - ISO_LOCAL_DATE (format "yyyy-MM-dd"), example '2013-12-17'
  - **TIME**
    - ISO_LOCAL_TIME (format "HH:mm:ss"), example '08:43:09'
  - **TIMESTAMP**
    - ISO 8601 (format "yyyy-MM-dd'T'HH:mm:ss'Z'"), example '2001-01-01T00:00:00Z'
  - etc.
