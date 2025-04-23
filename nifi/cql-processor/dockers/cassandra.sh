#!/bin/sh

docker network create testnet
docker pull cassandra:5.0.4
docker run --name cassandra -p 9042:9042 -p 7199:7199 -d --network testnet cassandra:5.0.4

