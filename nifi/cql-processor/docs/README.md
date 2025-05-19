## 1. Usage in NiFi

You can use NiFi v2 processor with controller (nifi-cql-nar-*.nar).
- ✅ [**PutCQL**](#3-putcql-nifi-processor), put data to CQL solution, where inputs are FlowFiles
- ✅ [**GetCQL**](#4-getcql-nifi-processor), get data from CQL solution, where outputs are FlowFiles

### 1.1 Preconditions for usage

You have to do these steps (only one-time action):
1. You need the NAR file 'nifi-cql-nar-*.nar'
    - the last version is [here](./../output/)
2. Import the *.nar file to the NiFi lib directory
    - expected location in Linux e.g. '/opt/nifi/current-nifi/lib'
    - expected location in Windows e.g. 'C:\Program Files\Apache\Nifi\lib'
3. STOP NiFi
4. START NiFi

## 2. CQLControllerService (NiFi controller)

![CQLControllerService, select processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_controller_service_add.png)

![CQLControllerService](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_controller_service_detail.png)

![CQLControllerService, properties](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_controller_service_properties.png)

### CQLControllerService, key items:

- **IP Addresses:**
    - IP addresses of CQL engine with comma delimiter e.g. '10.129.53.10,
      10.129.53.11, 10.129.53.12' or 'localhost', etc.
- **Port:**
    - Port for communication with CQL engine (default is 9042).
- **Secure Connection Bundle:**
    - Secure Connection Bundle for access to AstraDB (it is the link to '*.zip'
      file, downloaded from AstraDB web).
    - NOTE: the 'username' is 'clientId' and 'password' id 'secret', these values
      are from the file '*-token.json', downloaded from AstraDB web.
- **Username:**
    - Username for login to CQL.
- **Password:**
    - Password for login to CQL.
- **Local Data Center:**
    - Name of local data center in CQL typically e.g. 'dc1' or 'datacenter1', etc.
- **Connection Timeout:**
    - Connection timeout in seconds (default is 900 seconds).
- **Request Timeout:**
    - Request timeout in seconds (default is 60 seconds).
- **Consistency Level:**
    - Default consistency level (e.g. LOCAL_ONE, LOCAL_QUORUM, etc.,
      default is LOCAL_ONE).
- **SSL Context Service:**
    - SSL context service for CQL connection (default is without setting).

## 3. PutCQL (NiFi processor)

![PutCQL, select processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_put_processor_add.png)

![PutCQL, processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_put_processor.png)

![PutCQL, properties](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_put_processor_properties.png)

### Input

- **FlowFile with CSV content** for import (content [see](../../../docs/conversion.md)),
   where the CSV content (with header) is based on 'keyspace.table' definition in 
   CQL (from data types point of view).

### Output

- **cql.write.count** (FlowFile attribute)
  - The amount of write rows to CQL.

### PutCQL setting, key items:

- **Service Controller:**
  - see relation to [CQLControllerService](#2-cqlcontrollerservice-nifi-controller)
- **Write Consistency Level:**
  - Consistency level for write operation (e.g. LOCAL_ONE, LOCAL_QUORUM, etc.,
    default is LOCAL_ONE).
- **Table:**
  - Schema and table name in CQL for write/put a data (expected format 'keyspace.table', e.g. 'cqlschema.cqltable').
  - The data types defined in the CQL table will be used for value conversions from the CSV file.
- **Batch Size:**
  - Size of batch for write to CQL (default is 200).
- **Batch Type:**
  - Batch type with relation to an atomicity of batch operation (default UNLOGGED, it is without atomicity).
- **Dry Run:**
  - The simulation of write to CQL (default is false).

## 4. GetCQL (NiFi processor)

![GetCQL, select processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_get_processor_add.png)

![GetCQL, processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_get_processor.png)

![GetCQL, properties](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_get_processor_properties.png)

### Input

- **FlowFile** as optional.

### Output

- **cql.read.count** (FlowFile attribute)
    - The amount of read rows from CQL.

### GetCQL setting, key items:

- **Service Controller:**
    - see relation to [CQLControllerService](#2-cqlcontrollerservice-nifi-controller)
- **Read Consistency Level:**
    - Consistency level for write operation (e.g. LOCAL_ONE, LOCAL_QUORUM, etc.,
      default is LOCAL_ONE).
- **Table:**
    - Schema and table name in CQL for read/get a data (expected format 'keyspace.table', e.g. 'cqlschema.cqltable').
    - The data types defined in the CQL table will be used for value conversions to the CSV file.
- **Columns to Return:**
    - A comma-separated list of column names to be used in the query. If your CQL requires 
      special treatment of the names (quoting, e.g.), each name should include such treatment. If no
      column names are supplied, all columns in the specified table will be returned. NOTE: It is important
      to use consistent column names for a given table for incremental fetch to work properly.
- **Additional WHERE clause:**
    - A custom clause to be added in the WHERE condition when building CQL queries.
- **Custom Query:**
    - A custom CQL query used to retrieve data. Instead of building a CQL query from other
      properties, this query will be wrapped as a sub-query. Query must have no ORDER BY statement.

## 5. Sample flow definitions

The sample flow definition, [see](./nifi/cql-processor/docs/flow.md)