#!/bin/sh

# get/run '5.0.5' cassandra

docker network create testnet
docker pull cassandra:5.0.5
docker run --name cassandra -p 9042:9042 -p 7199:7199 -d --network testnet cassandra:5.0.5

# interactive access to the container
#    `docker exec -it cassandra cqlsh`
# or `docker exec -it cassandra bash`
# or `docker exec -it cassandra sh`
