## 1. Usage in NiFi

You can use NiFi processor (nifi-cql-nar-*.nar).
- ✅ **PutCQL** as NiFi processor, where input are FlowFiles

## 2. PutCQL (NiFi processor)

### 2.1 Preconditions for PutCQL usage

You have to do these steps (it is only one-time action):
 1. You need the processor file 'nifi-cql-nar-*.nar'
    - the last version is [here](./../output/)
 2. Import the *.nar file to the NiFi lib directory
    - expected location in Linux e.g. '/opt/nifi/current-nifi/lib'
 3. STOP NiFi
 4. START NiFi
 
### 2.2 Select processor
![PutCQL, select processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_putcql_add_processor.png)

### 2.3 Processor
![PutCQL, processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_putcql_processor.png)

### 2.4 Setup processor properties
![PutCQL, setup properties](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_putcql_properties.png)

### Input
- **FlowFile with CSV content** for import (content [see](../../../docs/conversion.md)),
   where the CSV content (with header) is based on 'keyspace.table' definition in 
   CQL (from data types point of view).

### Output
- **CQLCount** (FlowFile attribute)
  - Amount of write rows to CQL.
- **CQLCompareStatus** (FlowFile attribute)
  - View to the internal CQL processing (possible states SAME, CHANGE, CHANGE_ACCESS).

### PutCQL setting (key items):
- **IP Addresses:**
  - IP addresses of CQL engine with comma delimiter e.g. '10.129.53.159, 10.129.53.154, 10.129.53.153'
- **Port:**
  - Port for communication with CQL engine (default is 9042) 
- **Username:**
  - Username for login to CQL
- **Password:**
  - Password for login to CQL
- **Local Data Center:**
  - Name of local data center in CQL typically e.g. 'dc1' or 'datacenter1', etc.
- **Connection Timeout:**
  - 900 (in seconds)
- **Request Timeout:**
  - 60 (in seconds)
- **Consistency Level:**
  - E.g. LOCAL_ONE, LOCAL_QUORUM, etc.
- **Table:**
  - Schema and table name in CQL for write/put a data (expected format 'keyspace.table') 
    e.g. 'cqlschema.cqltable'
  - The data types defined in the table will be used for value conversions from the CSV file.
- **Batch Size:**
  - Size of batch for write to CQL (default is 200)
- **Dry Run:**
  - The simulation of write to CQL (default is false)
