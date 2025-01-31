## 1. Usage in NiFi
[README.md](../../../README.md)
You can use NiFi processor (nifi-cql-nar-*.nar).
- ✅ **PutCQL** as NiFi processor, where input are FlowFiles [see](#2-cqlprocessor)

### 2 PutCQL (NiFi processor)

#### Select processor
![PutCQL, add processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_putcql_add_processor.png)

#### Processor
![PutCQL, processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_putcql_processor.png)

#### Define properties
![PutCQL, properties](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_putcql_properties.png)

#### Input:
1. **FlowFile with CSV content** for import (content [see](../../../README.md#31-expected-contentformat)),
   where the CSV content is based on 'keyspace.table' definition in CQL

#### PutCQL setting (key items):
- **IP Addresses:**
  - ip addresses with comma delimiter e.g. '10.129.53.159, 10.129.53.154, 10.129.53.153'
- **Port**
  - 9042 
- **Username**
  - username for login to CQL
- **Password**
  - password for login to CQL
- **Local Data Center**
  - name of local data center typically e.g. 'dc1' or 'datacenter1', etc.
- **Connection Timeout**
  - 900 (in seconds)
- **Request Timeout**
  - 60 (in seconds)
- **Consistency Level**
  - e.g. LOCAL_ONE, LOCAL_QUORUM, etc.
- **Table**
  - Schema and table name in CQL for write/put a data (expected format 'keyspace.table') 
    e.g. 'cqlschema.cqltable' 
- **Batch Size**
  - size of batch (default is 200)
- **Dry Run**
  - false (the simulation of write to CQL, default is true)
