#!/bin/bash

# Set Neo4j database password
sudo -u neo4j neo4j-admin set-initial-password darpatheia1
# Start Neo4j service
service neo4j start

# Set up Anomaly database
echo "create database \"anomaly.db\"; create user theia with encrypted password 'darpatheia1'; grant all privileges on database \"anomaly.db\" to theia;" | sudo -u postgres psql
sudo -u theia psql -d anomaly.db -f /usr/share/theia/create.sql
