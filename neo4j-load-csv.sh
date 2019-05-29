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
    count=$(ls -1 $BASE | wc -l)
    for file_num in `ls -1 $BASE | sed 's/.*-\(.*\)/\1/' | sort -n`; do
        file="${BASE:0:-1}${file_num}"

        if [[ -f "$file" ]]; then
            # Check if we need to exit
            if [[ $count -eq 1 ]]; then
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

            # Decrement count
            count=$((count-1))
        fi
    done

    echo '==============================================='
    echo '==============================================='
}

if [[ "$#" -ne 2 ]]; then
    echo "usage: neo4j-load-csv.sh handler_neo4j.cfg id"
    exit 2
fi

if [[ `id -u` -ne 0 ]]; then
    echo "Need to be root"
    exit 1
fi

# Create lockfile so this script isn't run twice simultaneously
# https://stackoverflow.com/questions/185451/quick-and-dirty-way-to-ensure-only-one-instance-of-a-shell-script-is-running-at?page=1&tab=votes#tab-top
LOCKFILE=/tmp/neo4jlock-$2.txt
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

# Run query on principal nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///principal-node-$2.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.userId = line[2], n.name = line[3]
ON MATCH SET n.nodeType = line[0], n.userId = line[2], n.name = line[3];
\""
query $archive "$root/principal-node-*" "\${QUERY}" ${IMPORT_DIR}/principal-node-$2.csv

# Run query on subject nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///subject-node-$2.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.type = line[2], n.cid = line[3], n.tgid = line[4], n.parent_subject = line[5], n.local_principal = line[6], n.ts = line[7], n.cmdline = line[8], n.name = line[9]
ON MATCH SET n.nodeType = line[0], n.type = line[2], n.cid = line[3], n.tgid = line[4], n.parent_subject = line[5], n.local_principal = line[6], n.ts = line[7], n.cmdline = line[8], n.name = line[9];
\""
query $archive "$root/subject-node-*" "\${QUERY}" ${IMPORT_DIR}/subject-node-$2.csv

# Run query on subject nodes to update them in the event of execve
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///subject-node-update-$2.csv' as line
MERGE (n:NODE {uuid: line[0]})
ON MATCH SET n.cmdline = line[1], n.name = n.cid + ':' + line[1];
\""
query $archive "$root/subject-update-*" "\${QUERY}" ${IMPORT_DIR}/subject-node-update-$2.csv

# Run query on file nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///file-node-$2.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.local_principal = line[2], n.filename = line[3], n.name = line[4]
ON MATCH SET n.nodeType = line[0], n.local_principal = line[2], n.filename = line[3], n.name = line[4];
\""
query $archive "$root/file-node-*" "\${QUERY}" ${IMPORT_DIR}/file-node-$2.csv

# Run query on netflow nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///netflow-node-$2.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.local_address = line[2], n.local_port = line[3], n.remote_address = line[4], n.remote_port = line[5], n.name = line[6]
ON MATCH SET n.nodeType = line[0], n.local_address = line[2], n.local_port = line[3], n.remote_address = line[4], n.remote_port = line[5], n.name = line[6];
\""
query $archive "$root/netflow-node-*" "\${QUERY}" ${IMPORT_DIR}/netflow-node-$2.csv

# Run query on ipc nodes
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///ipc-node-$2.csv' as line
MERGE (n:NODE {uuid: line[1]})
ON CREATE SET n.nodeType = line[0], n.type = line[2], n.name = line[3]
ON MATCH SET n.nodeType = line[0], n.type = line[2], n.name = line[3]
\""
query $archive "$root/ipc-node-*" "\${QUERY}" ${IMPORT_DIR}/ipc-node-$2.csv

# Run query on backward edges
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///backward-edge-$2.csv' as line
MERGE (n1:NODE {uuid: line[3]})
MERGE (n2:NODE {uuid: line[4]})
WITH line,n1,n2
WHERE NOT n1.uuid ENDS WITH '00000000-0000-0000-0000-000000000000' AND NOT n2.uuid ENDS WITH '00000000-0000-0000-0000-000000000000'
CREATE (n1)<-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[5], size:line[6], name:line[7]}]-(n2)
\""
query $archive "$root/backward-edge-*" "\${QUERY}" ${IMPORT_DIR}/backward-edge-$2.csv

# Run query on forward edges
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///forward-edge-$2.csv' as line
MERGE (n1:NODE {uuid: line[3]})
MERGE (n2:NODE {uuid: line[4]})
WITH line,n1,n2
WHERE NOT n1.uuid ENDS WITH '00000000-0000-0000-0000-000000000000' AND NOT n2.uuid ENDS WITH '00000000-0000-0000-0000-000000000000'
CREATE (n1)-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[5], size:line[6], name:line[7]}]->(n2)
\""
query $archive "$root/forward-edge-*" "\${QUERY}" ${IMPORT_DIR}/forward-edge-$2.csv

# Run query on clone edges
# All edges need to be forward, except when pid and tgid are different
# Then they need to be bi-directional
QUERY="\"
USING PERIODIC COMMIT 500
LOAD CSV FROM 'file:///clone-edge-$2.csv' as line
MERGE (n1:NODE {uuid: line[3]})
MERGE (n2:NODE {uuid: line[4]})
WITH line,n1,n2
WHERE NOT n1.uuid ENDS WITH '00000000-0000-0000-0000-000000000000' AND NOT n2.uuid ENDS WITH '00000000-0000-0000-0000-000000000000'
CREATE (n1)-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[5], size:line[6], name:line[7]}]->(n2)
WITH line,n1,n2
WHERE NOT n2.cid = n2.tgid
CREATE (n1)<-[:NODE {uuid:line[1], nodeType:line[0], type:line[2], ts:line[5], size:line[6], name:line[7]}]-(n2)
\""
query $archive "$root/clone-edge-*" "\${QUERY}" ${IMPORT_DIR}/clone-edge-$2.csv

# Remove lockfile
rm -f ${LOCKFILE}
