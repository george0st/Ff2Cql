## Sample flow definitions

### 1. PutCQL (with ScyllaDB)
The easy sample with PutCQL (write/put data to ScyllaDB,
see controller setting) [see](../flows/Test-PutCQL.json).

### 2. GetCQL (with ScyllaDB)
The easy sample with GetCQL (read/get data from ScyllaDB,
see controller setting) [see](../flows/Test-GetCQL.json).

### 3. PutCQL & GetCQL (with ScyllaDB)
TBD.

### NOTE: Load/Use Flow definition 

If you need to use JSON file with flow definition in Apache NiFi, follow next steps:

1. Choose 'Process Group' and you will see dialog with title 'Create Process Group'
2. Click to the button 'Browse' (you can see in the dialog, the edit box 'Name' the browse icon) and 
  choose *.json file
3. Click to the button 'Add'
4. In case of load/create Controller (as part of the flow), you have to typically:
    - Enable this controller (default is disable), before start flow
