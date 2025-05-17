#!/bin/sh

# get/run '2.4.0' nifi

docker network create testnet
docker pull apache/nifi:2.4.0
docker run --name nifi -p 8443:8443 -d --network testnet -e SINGLE_USER_CREDENTIALS_USERNAME=test -e SINGLE_USER_CREDENTIALS_PASSWORD=testUserXNifiv2 apache/nifi:2.4.0

# https://localhost:8443/nifi/
# username: test
# pwd:      testUserXNifiv2
