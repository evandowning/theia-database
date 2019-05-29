#!/bin/bash

user=`whoami`

# Set Neo4j database password
sudo -u neo4j neo4j-admin set-initial-password darpatheia1
# Start Neo4j service
service neo4j start

# Set up Anomaly database
echo "create database \"anomaly.db\"; create user $user with encrypted password 'darpatheia1'; grant all privileges on database \"anomaly.db\" to $user;" | sudo -u postgres psql
sudo -u $user psql -d anomaly.db -f /usr/share/theia/create.sql
