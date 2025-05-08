#!/bin/sh

# get/run '2024.2.2.2-b2' yugabyte

docker network create testnet
docker pull yugabytedb/yugabyte:2024.2.2.2-b2
docker run --name yugabyte -p 9042:9042 -p 7199:7199 -d --network testnet yugabytedb/yugabyte:2024.2.2.2-b2

