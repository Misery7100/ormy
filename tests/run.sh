#!/bin/bash

# Get the directory of this script 
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Run docker compose
docker-compose -f $DIR/compose/docker-compose.yml up --build -d

echo "Sleeping for 10 seconds..." && sleep 10
echo "Substitute version" && poetry dynamic-versioning

# Run tests
echo "Run tests" && ./.venv/bin/tox

docker-compose -f $DIR/compose/docker-compose.yml down