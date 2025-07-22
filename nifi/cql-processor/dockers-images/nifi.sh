#!/bin/sh

# get/run '2.5.0' nifi

docker network create testnet
docker pull apache/nifi:2.5.0
docker run --name nifi -p 8443:8443 -d --network testnet -e SINGLE_USER_CREDENTIALS_USERNAME=test -e SINGLE_USER_CREDENTIALS_PASSWORD=testUserXNifiv2 apache/nifi:2.5.0


# Usage
########

# 1. open nifi in browner
# https://localhost:8443/nifi/
# username: test
# pwd:      testUserXNifiv2

# 2. use 'nifi-cql-nar-*.nar' in nifi
# 2.1 write the '*.nar' file to directory '/opt/nifi/nifi-current/lib'
# 2.2 restart nifi