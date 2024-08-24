#!/bin/bash
set -e

# Wait for MongoDB to be fully up and ready to accept connections
until mongosh --host mongodb:27017 -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --eval "print('wait for connection')" &>/dev/null; do
  echo 'Waiting for MongoDB to connect...'
  sleep 2
done

echo 'MongoDB connected! Initiating replica set...'

# Initiate the replica set
mongosh --host mongodb:27017 -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --eval "
config = {
  '_id': 'rs0',
  'members': [
    {
      '_id': 0,
      'host': 'mongodb:27017'
    }
  ]
};
rs.initiate(config);
"

echo 'Replica set initiated.'