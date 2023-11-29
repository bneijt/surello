#!/bin/bash
mkdir -p tmp/db

# If the .env file does not exist, create it with a random password
if [ ! -f .env ]; then
	echo "Creating .env file with random password"
	echo "SURREALDB_PASS=\"$(openssl rand -base64 32)\"" > .env
	echo "SURREALDB_USER=\"admin\"" >> .env
	echo "SURREALDB_ADDRESS=\"127.0.0.1:8000\"" >> .env
	echo "SURREALDB_NAMESPACE=\"surello_test\"" >> .env
	echo "SURREALDB_DATABASE=\"main\"" >> .env
fi
source .env

docker run --rm --user 1000:1000 -p 8000:8000 -v "$(pwd)/tmp/db:/mydata" \
	surrealdb/surrealdb:latest start \
	--log info --auth --user "$SURREALDB_USER" --pass "$SURREALDB_PASS" \
	file:/mydata/${SURREALDB_NAMESPACE}.db

echo "Closed"

