#!/bin/bash

# Runs queries on all but the last file in the folder (in case it's still being
# written to).
function query() {
    # Get parameters
    archive=$1
    BASE=$2
    eval QUERY_="$3"
    OUT=$4

#   echo $archive
#   echo $BASE
#   echo "${QUERY_}"
#   echo $OUT
#   echo ''

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

if [[ "$#" -ne 2 ]]; then
    echo "usage: ./neo4j-load-csv.sh handler_neo4j.cfg host_uuid"
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
# Get host uuid
host_uuid=$2

# From: https://stackoverflow.com/questions/19271959/how-can-i-check-the-last-character-in-a-string-in-bash
root=$csv_directory
if [[ "$root" == */ ]]; then
  root="${root:0:${#root}-1}_${host_uuid}/"
else
  root="${root}_${host_uuid}"
fi

archive=$archive_directory
if [[ "$archive" == */ ]]; then
  archive="${archive:0:${#archive}-1}_${host_uuid}/"
else
  archive="${archive}_${host_uuid}"
fi

IMPORT_DIR="/data/neo4j-csvs"

# Create folder if it doesn't already exist
mkdir -p $IMPORT_DIR
mkdir -p $root
mkdir -p $archive

CYPHER_BIN="cypher-shell"
USER="neo4j"
PASS="neo4jtheia1"
CYPHER_ARGS="-u $USER -p $PASS"

QUERY="\"CREATE CONSTRAINT ON (n:NODE) ASSERT n.uuid IS UNIQUE\""
eval "${CYPHER_BIN}" "${CYPHER_ARGS}" "${QUERY}"

# Run query on backward edges
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///backward-edge_${host_uuid}.csv' as line
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
query $archive "$root/backward-edge-*" "\${QUERY}" ${IMPORT_DIR}/backward-edge_${host_uuid}.csv

# Run query on forward edges
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///forward-edge_${host_uuid}.csv' as line
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
query $archive "$root/forward-edge-*" "\${QUERY}" ${IMPORT_DIR}/forward-edge_${host_uuid}.csv

# Run query on principal nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///principal-node_${host_uuid}.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.userId = line[2], n.name = line[3]
ON MATCH SET n.nodeType = line[0], n.userId = line[2], n.name = line[3];
\""
query $archive "$root/principal-node-*" "\${QUERY}" ${IMPORT_DIR}/principal-node_${host_uuid}.csv

# Run query on subject nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///subject-node_${host_uuid}.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.type = line[2], n.cid = line[3], n.parent_subject = line[4], n.local_principal = line[5], n.ts = line[6], n.cmdline = line[7], n.name = line[8]
ON MATCH SET n.nodeType = line[0], n.type = line[2], n.cid = line[3], n.parent_subject = line[4], n.local_principal = line[5], n.ts = line[6], n.cmdline = line[7], n.name = line[8];
\""
query $archive "$root/subject-node-*" "\${QUERY}" ${IMPORT_DIR}/subject-node_${host_uuid}.csv

# Run query on subject nodes to update them in the event of execve
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///subject-node-update_${host_uuid}.csv' as line
MERGE (n:NODE {uuid: line[0]})
ON MATCH SET n.cmdline = line[1], n.name = n.cid + ':' + line[1];
\""
query $archive "$root/subject-update-*" "\${QUERY}" ${IMPORT_DIR}/subject-node-update_${host_uuid}.csv

# Run query on file nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///file-node_${host_uuid}.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.local_principal = line[2], n.filename = line[3], n.name = line[4]
ON MATCH SET n.nodeType = line[0], n.local_principal = line[2], n.filename = line[3], n.name = line[4];
\""
query $archive "$root/file-node-*" "\${QUERY}" ${IMPORT_DIR}/file-node_${host_uuid}.csv

# Run query on netflow nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///netflow-node_${host_uuid}.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.local_address = line[2], n.local_port = line[3], n.remote_address = line[4], n.remote_port = line[5], n.name = line[6]
ON MATCH SET n.nodeType = line[0], n.local_address = line[2], n.local_port = line[3], n.remote_address = line[4], n.remote_port = line[5], n.name = line[6];
\""
query $archive "$root/netflow-node-*" "\${QUERY}" ${IMPORT_DIR}/netflow-node_${host_uuid}.csv

# Run query on ipc nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///ipc-node_${host_uuid}.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.type = line[2], n.name = line[2]
ON MATCH SET n.nodeType = line[0], n.type = line[2], n.name = line[2]
\""
query $archive "$root/ipc-node-*" "\${QUERY}" ${IMPORT_DIR}/ipc-node_${host_uuid}.csv

# Remove lockfile
rm -f ${LOCKFILE}
