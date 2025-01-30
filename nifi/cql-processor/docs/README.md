## 1. Usage in NiFi

You can use NiFi processor (nifi-cql-nar-*.nar).
- ✅ **PutCql** as NiFi processor, where input are FlowFiles [see](#2-cqlprocessor)

### 2 PutCql (NiFi processor)

![PutCql Nifi processor](https://github.com/george0st/Csv2Cql/blob/main/nifi/cql-processor/docs/assets/nifi_putcql.png)

#### Input:
1. **FlowFile with CSV content** for import (content [see](../../../README.md#31-expected-contentformat)),
   where the CSV content is based on 'keyspace.table' definition in CQL

#### PutCql setting (key items):
- **IP Addresses:**
  - ip addresses e.g. '10.129.53.159, 10.129.53.154, 10.129.53.153'
- **Port**
  - 9042 
- **Username**
  - username for login to CQL
- **Password**
  - password for login to CQL
- **Local Data Center**
  - name of local data center e.g. 'dc1' or 'datacenter1', etc.
- **Connection Timeout**
  - 900 (seconds)
- **Request Timeout**
  - 60 (seconds)
- **Consistency Level**
  - e.g. LOCAL_ONE, LOCAL_QUORUM, etc.
- **Table**
  - Table and schema name in CQL (expected format 'keyspace.table') 
    e.g. 'prfschema.table' 
- **Batch Size**
  - size of batch, default is 200
- **Dry Run**
  - false - the simulation of write to CQL
