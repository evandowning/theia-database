#!/bin/bash

# Runs queries on all but the last file in the folder (in case it's still being
# written to).
function query() {
    # Get parameters
    # Folder to archive loaded CSV files to
    archive=$1
    # Folder which stores CSV files to be copied and loaded
    BASE=$2
    # Query to load CSV file to Neo4j
    eval QUERY_="$3"
    # Folder which stores CSV files to be loaded into Neo4j
    OUT=$4

    # Run queries from each CSV file
    count=1
    max=`ls -1 $BASE | wc -l`
    for file in `ls -1 $BASE | sort`; do
        if [[ -f $file ]]; then
            # Check if we need to exit
            if [[ $max -le $count ]]; then
                break
            fi

            echo $file

            # Copy file
            cp $file $OUT

            # Load CSV file into Neo4j
            cl="${CYPHER_BIN} ${CYPHER_ARGS} $QUERY_"
            echo $cl
            if eval $cl; then
                # Archive file only if command was successful
                mv $file $archive
            fi

            # Increment count
            count=$((count+1))
        fi
    done

    echo '==============================================='
    echo '==============================================='
}

if [[ "$#" -ne 1 ]]; then
    echo "usage: neo4j-load-csv.sh handler_neo4j.cfg"
    exit 2
fi

if [[ `id -u` -ne 0 ]]; then
    echo "Need to be root"
    exit 1
fi

# Create lockfile so this script isn't run twice simultaneously
# https://stackoverflow.com/questions/185451/quick-and-dirty-way-to-ensure-only-one-instance-of-a-shell-script-is-running-at?page=1&tab=votes#tab-top
LOCKFILE=/tmp/neo4jlock.txt
if [ -e ${LOCKFILE} ] && kill -0 `cat ${LOCKFILE}`; then
    echo "already running"
    exit
fi

# make sure the lockfile is removed when we exit and then claim it
trap "rm -f ${LOCKFILE}; exit" INT TERM EXIT
echo $$ > ${LOCKFILE}



# Get parameters from config file
cfg=$1
if [ -f $cfg ]; then
    . $cfg
fi

# Get CSV directories
root=$csv_directory
archive=$archive_directory

IMPORT_DIR="/var/lib/neo4j/import"

# Create folder if it doesn't already exist
mkdir -p $IMPORT_DIR
mkdir -p $root
mkdir -p $archive

CYPHER_BIN="cypher-shell"
USER="neo4j"
PASS="darpatheia1"
CYPHER_ARGS="-u $USER -p $PASS"

QUERY="\"CREATE CONSTRAINT ON (n:NODE) ASSERT n.uuid IS UNIQUE\""
eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"

# Run query on backward edges
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///backward-edge.csv' as line
MERGE (n1:NODE {uuid: line[3]})
MERGE (n2:NODE {uuid: line[4]})
MERGE (n3:NODE {uuid: line[5]})
WITH line,n1,n2,n3
WHERE n1.uuid <> '0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0' AND n2.uuid <> '0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0'
CREATE (n1)<-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[6], size:line[7], name:line[8]}]-(n2)
WITH line,n1,n3
WHERE n1.uuid <> '0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0' AND n3.uuid <> '0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0'
CREATE (n1)<-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[6], size:line[7], name:line[8]}]-(n3);
\""
query $archive "$root/backward-edge-*" "\${QUERY}" ${IMPORT_DIR}/backward-edge.csv

# Run query on forward edges
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///forward-edge.csv' as line
MERGE (n1:NODE {uuid: line[3]})
MERGE (n2:NODE {uuid: line[4]})
MERGE (n3:NODE {uuid: line[5]})
WITH line,n1,n2,n3
WHERE n1.uuid <> '0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0' AND n2.uuid <> '0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0'
CREATE (n1)-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[6], size:line[7], name:line[8]}]->(n2)
WITH line,n1,n3
WHERE n1.uuid <> '0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0' AND n3.uuid <> '0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0'
CREATE (n1)-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[6], size:line[7], name:line[8]}]->(n3);
\""
query $archive "$root/forward-edge-*" "\${QUERY}" ${IMPORT_DIR}/forward-edge.csv

# Run query on principal nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///principal-node.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.userId = line[2], n.name = line[3]
ON MATCH SET n.nodeType = line[0], n.userId = line[2], n.name = line[3];
\""
query $archive "$root/principal-node-*" "\${QUERY}" ${IMPORT_DIR}/principal-node.csv

# Run query on subject nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///subject-node.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.type = line[2], n.cid = line[3], n.parent_subject = line[4], n.local_principal = line[5], n.ts = line[6], n.cmdline = line[7], n.name = line[8]
ON MATCH SET n.nodeType = line[0], n.type = line[2], n.cid = line[3], n.parent_subject = line[4], n.local_principal = line[5], n.ts = line[6], n.cmdline = line[7], n.name = line[8];
\""
query $archive "$root/subject-node-*" "\${QUERY}" ${IMPORT_DIR}/subject-node.csv

# Run query on subject nodes to update them in the event of execve
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///subject-node-update.csv' as line
MERGE (n:NODE {uuid: line[0]})
ON MATCH SET n.cmdline = line[1], n.name = n.cid + ':' + line[1];
\""
query $archive "$root/subject-update-*" "\${QUERY}" ${IMPORT_DIR}/subject-node-update.csv

# Run query on file nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///file-node.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.local_principal = line[2], n.filename = line[3], n.name = line[4]
ON MATCH SET n.nodeType = line[0], n.local_principal = line[2], n.filename = line[3], n.name = line[4];
\""
query $archive "$root/file-node-*" "\${QUERY}" ${IMPORT_DIR}/file-node.csv

# Run query on netflow nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///netflow-node.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.local_address = line[2], n.local_port = line[3], n.remote_address = line[4], n.remote_port = line[5], n.name = line[6]
ON MATCH SET n.nodeType = line[0], n.local_address = line[2], n.local_port = line[3], n.remote_address = line[4], n.remote_port = line[5], n.name = line[6];
\""
query $archive "$root/netflow-node-*" "\${QUERY}" ${IMPORT_DIR}/netflow-node.csv

# Run query on ipc nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///ipc-node.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.type = line[2], n.name = line[2]
ON MATCH SET n.nodeType = line[0], n.type = line[2], n.name = line[2]
\""
query $archive "$root/ipc-node-*" "\${QUERY}" ${IMPORT_DIR}/ipc-node.csv

# Remove lockfile
rm -f ${LOCKFILE}
