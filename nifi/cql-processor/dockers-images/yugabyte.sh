#!/bin/sh

# get/run '2.25.1.0-b381' yugabyte

docker network create testnet
docker pull yugabytedb/yugabyte:2.25.1.0-b381
docker run --name yugabyte -p 7000:7000 -p 9000:9000 -p 15433:15433 -p 5433:5433 -p 9042:9042 -d --network testnet yugabytedb/yugabyte:2.25.1.0-b381 bin/yugabyted start --background=false

# Usage
########

# 1. show yugabyte status
# docker exec -it yugabyte yugabyted status

# 2. run ycqlsh (yugabyte CQL)
# docker exec -it yugabyte bash -c "bin/ycqlsh yugabyte 9042 -u cassandra"

# 3. web UI (yugabyte administration)
# http://localhost:15433/

# 4. run ysqlsh (yugabyte SQL)
# docker exec -it yugabyte bash -c "bin/ysqlsh -h yugabyte -U yugabyte -d yugabyte"