#!/bin/bash

if [[ "$#" -ne 1 ]]; then
    echo "usage: ./neo4j-load-csv.sh data/csv-folder &> out"
    exit 2
fi

if [[ `id -u` -ne 0 ]]; then
    echo "Need to be root"
    exit 1
fi

# Get parameter
root=$1
IMPORT_DIR="/data/neo4j-csvs"
CYPHER_BIN="cypher-shell"
NEO4J_SERVER="143.215.130.71:7687"
USER="neo4j"
CYPHER_ARGS="-a $NEO4J_SERVER -u neo4j -p theianeo4j1"

QUERY="\"CREATE CONSTRAINT ON (n:NODE) ASSERT n.uuid IS UNIQUE\""
eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"

# Get list of CSV files
for file in $root/backward-edge-*; do
    if [[ -f $file ]]; then
        echo $file

	# Copy file
	cp $file $IMPORT_DIR/backward-edge.csv
# "EVENT","119 166 30 47 183 56 76 21 27 0 0 0 0 0 0 16","EVENT_READ_SOCKET_PARAMS","209 7 13 0 0 0 0 0 0 0 0 0 0 0 0 32","253 255 255 255 0 0 192 168 122 19 0 0 0 0 0 64","0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0","1534663932447204983","4096","EVENT_READ_SOCKET_PARAMS"

	QUERY="\"
	USING PERIODIC COMMIT 500
	LOAD CSV FROM 'file:///backward-edge.csv' as line
	MERGE (n1:NODE {uuid: line[3]})
	MERGE (n2:NODE {uuid: line[4]})
	MERGE (n3:NODE {uuid: line[5]})
	WITH line,n1,n2,n3
	CREATE (n1)<-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[6], size:line[7], name:line[8]}]-(n2)
	WITH line,n1,n3
	CREATE (n1)<-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[6], size:line[7], name:line[8]}]-(n3);
	\""


	# Load CSV file into Neo4j
	#echo "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
	time eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
    fi
done
# Get list of CSV files
for file in $root/forward-edge-*; do
    if [[ -f $file ]]; then
        echo $file

	# Copy file
	cp $file $IMPORT_DIR/forward-edge.csv

	QUERY="\"
	USING PERIODIC COMMIT 500
	LOAD CSV FROM 'file:///forward-edge.csv' as line
	MERGE (n1:subject {uuid: line[3]})
	MERGE (n2:NODE {uuid: line[4]})
	MERGE (n3:NODE {uuid: line[5]})
	WITH line,n1,n2,n3
	CREATE (n1)-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[6], size:line[7], name:line[8]}]->(n2)
	WITH line,n1,n3
	CREATE (n1)-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[6], size:line[7], name:line[8]}]->(n3);
	\""

	# Load CSV file into Neo4j
	#echo "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
	time eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
    fi
done

# Get list of CSV files
for file in $root/principal-node-*; do
    if [[ -f $file ]]; then
        echo $file

        # Copy file
	cp $file $IMPORT_DIR/principal-node.csv

        QUERY="\"
        USING PERIODIC COMMIT 500
        LOAD CSV FROM 'file:///principal-node.csv' as line
        MERGE (n:principal {uuid: line[1]})
        ON CREATE SET n.nodeType = line[0], n.userId = line[2], n.name = line[3]
        ON MATCH SET n.nodeType = line[0], n.userId = line[2], n.name = line[3];
        \""

        # Load CSV file into Neo4j
        #echo "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
        time eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
    fi
done

# Get list of CSV files
for file in $root/subject-node-*; do
    if [[ -f $file ]]; then
        echo $file

        # Copy file
	cp $file $IMPORT_DIR/subject-node.csv

        QUERY="\"
        USING PERIODIC COMMIT 500
        LOAD CSV FROM 'file:///subject-node.csv' as line
        MERGE (n:subject {uuid: line[1]})
        ON CREATE SET n.nodeType = line[0], n.type = line[2], n.cid = line[3], n.parent_subject = line[4], n.local_principal = line[5], n.ts = line[6], n.cmdline = line[7], n.name = line[8]
        ON MATCH SET n.nodeType = line[0], n.type = line[2], n.cid = line[3], n.parent_subject = line[4], n.local_principal = line[5], n.ts = line[6], n.cmdline = line[7], n.name = line[8];
        \""

        # Load CSV file into Neo4j
        #echo "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
        time eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
    fi
done

# Get list of CSV files
for file in $root/file-node-*; do
    if [[ -f $file ]]; then
        echo $file

        # Copy file
	cp $file $IMPORT_DIR/file-node.csv

        QUERY="\"
        USING PERIODIC COMMIT 500
        LOAD CSV FROM 'file:///file-node.csv' as line
        MERGE (n:file {uuid: line[1]})
        ON CREATE SET n.nodeType = line[0], n.local_principal = line[2], n.filename = line[3], n.name = line[4]
        ON MATCH SET n.nodeType = line[0], n.local_principal = line[2], n.filename = line[3], n.name = line[4];
        \""

        # Load CSV file into Neo4j
        #echo "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
        time eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
    fi
done

# Get list of CSV files
for file in $root/netflow-node-*; do
    if [[ -f $file ]]; then
        echo $file

        # Copy file
	cp $file $IMPORT_DIR/netflow-node.csv

        QUERY="\"
        USING PERIODIC COMMIT 500
        LOAD CSV FROM 'file:///netflow-node.csv' as line
        MERGE (n:netflow {uuid: line[1]})
        ON CREATE SET n.nodeType = line[0], n.local_address = line[2], n.local_port = line[3], n.remote_address = line[4], n.remote_port = line[5], n.name = line[6]
        ON MATCH SET n.nodeType = line[0], n.local_address = line[2], n.local_port = line[3], n.remote_address = line[4], n.remote_port = line[5], n.name = line[6];
        \""

        # Load CSV file into Neo4j
        #echo "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
        time eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"
    fi
done
