#!/bin/bash

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd $DIR

DEPS=$(
cat $DIR/requirements.txt \
| xargs -n1 -I% echo --depends \'python-%\' \
| sed -e "s/\(>=[^']*\)/ (\1)/g" -e "s/==\([^']*\)/ (=\1)/g" \
)
eval $(echo fpm \
-s dir \
-t deb \
-n analysis-db-tools \
${DEPS} \
-v 1.0-0 \
create_neo4j_csv.py=/usr/bin/ \
anomaly_detector.py=/usr/bin/ \
theia_neo4j.py=/usr/bin/ \
sensitive.py=/usr/bin/ \
handler_neo4j.cfg=/etc/theia/ \
handler_anomaly.cfg=/etc/theia/ \
create.sql=/usr/share/theia/ \
clear.sql=/usr/share/theia/ \
neo4j.cron=/etc/cron.d/ \
neo4j-load-csv.sh=/usr/bin/ \
neo4j.conf=/etc/neo4j/
)

fpm -s python -t deb .

popd
