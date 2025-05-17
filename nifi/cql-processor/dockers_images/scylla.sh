#!/bin/sh

# get/run '2025.1.1' scylla

docker network create testnet
docker pull scylladb/scylla:2025.1.1
docker run --name scylla -p 9042:9042 -p 7199:7199 -d --network testnet scylladb/scylla:2025.1.1

