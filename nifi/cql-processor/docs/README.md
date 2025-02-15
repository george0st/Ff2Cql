## 1. Usage in NiFi

You can use NiFi processor (nifi-cql-nar-*.nar).
- ✅ **PutCQL** as NiFi v2 processor with controller, where inputs are FlowFiles

## 2. PutCQL (NiFi processor)

### 2.1 Preconditions for usage

You have to do these steps (only one-time action):
 1. You need the NAR file 'nifi-cql-nar-*.nar'
    - the last version is [here](./../output/)
 2. Import the *.nar file to the NiFi lib directory
    - expected location in Linux e.g. '/opt/nifi/current-nifi/lib'
    - expected location in Windows e.g. 'C:\Program Files\Apache\Nifi\lib'
 3. STOP NiFi
 4. START NiFi

### 2.2 Add controller and setup properties

![CQLControllerService, select processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_controller_service_add.png)

![CQLControllerService](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_controller_service_detail.png)

![CQLControllerService, properties](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_controller_service_properties.png)

### CQLControllerService setting (key items):

- **IP Addresses:**
    - IP addresses of CQL engine with comma delimiter e.g. '10.129.53.10, 10.129.53.11, 10.129.53.12'.
- **Port:**
    - Port for communication with CQL engine (default is 9042).
- **Username:**
    - Username for login to CQL.
- **Password:**
    - Password for login to CQL.
- **Local Data Center:**
    - Name of local data center in CQL typically e.g. 'dc1' or 'datacenter1', etc.
- **Connection Timeout:**
    - Connection timeout in seconds (deafult is 900 seconds).
- **Request Timeout:**
    - Request timeout in seconds (default is 60 seconds).
- **Consistency Level:**
    - Default consistency level (e.g. LOCAL_ONE, LOCAL_QUORUM, etc.).

### 2.3 Add processor and setup properties

![PutCQL, select processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_processor_add.png)

![PutCQL, processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_processor.png)

![PutCQL, properties](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_processor_properties.png)

### Input

- **FlowFile with CSV content** for import (content [see](../../../docs/conversion.md)),
   where the CSV content (with header) is based on 'keyspace.table' definition in 
   CQL (from data types point of view).

### Output

- **cql.count** (FlowFile attribute)
  - The amount of write rows to CQL.

### PutCQL setting (key items):

- **Write Consistency Level:**
  - Consistency level for write operation (e.g. LOCAL_ONE, LOCAL_QUORUM, etc.).
- **Table:**
  - Schema and table name in CQL for write/put a data (expected format 'keyspace.table', e.g. 'cqlschema.cqltable').
  - The data types defined in the CQL table will be used for value conversions from the CSV file.
- **Batch Size:**
  - Size of batch for write to CQL (default is 200).
- **Batch Type:**
  - Batch type with relation to an atomicity of batch operation (default UNLOGGED, it is without atomicity).
- **Dry Run:**
  - The simulation of write to CQL (default is false).
