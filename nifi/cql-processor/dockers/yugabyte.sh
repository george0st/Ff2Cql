#!/bin/sh

# get/run '2.25.1.0-b381' yugabyte

docker network create testnet
docker pull yugabytedb/yugabyte:2.25.1.0-b381
docker run --name yugabyte -p 7000:7000 -p 9000:9000 -p 15433:15433 -p 5433:5433 -p 9042:9042 -d --network testnet yugabytedb/yugabyte:2.25.1.0-b381 bin/yugabyted start --background=false


# show status
# docker exec -it yugabyte yugabyted status

# run ysqlsh
# docker exec -it yugabyte bash -c "bin/ysqlsh -h c82cf988b153  -U yugabyte -d yugabyte"

# web UI
# http://localhost:15433/