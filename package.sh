#!/bin/bash

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd $DIR

# Get python dependencies
DEPS=$(
cat $DIR/requirements.txt \
| xargs -n1 -I% echo --depends \'python-%\' \
| sed -e "s/\(>=[^']*\)/ (\1)/g" -e "s/==\([^']*\)/ (=\1)/g" \
)

echo $DEPS

# Create deb package for analysis-db-tools
eval $(echo fpm \
-s dir \
-t deb \
-n analysis-db-tools \
${DEPS} \
--depends default-jre --depends default-jre-headless --depends neo4j \
--depends \'postgresql \(\>=9.5\)\' --depends python-analysis-db-consumer \
--after-install "post.sh" \
-v 1.0-0 \
create_neo4j_csv.py=/usr/bin/ \
anomaly_detector.py=/usr/bin/ \
theia_neo4j.py=/usr/bin/ \
theia_anomaly.py=/usr/bin/ \
sensitive.py=/usr/bin/ \
etc/handler_neo4j.cfg=/etc/theia/ \
etc/handler_anomaly.cfg=/etc/theia/ \
etc/create.sql=/usr/share/theia/ \
etc/clear.sql=/usr/share/theia/ \
etc/neo4j.cron=/etc/cron.d/ \
neo4j-load-csv.sh=/usr/bin/ \
etc/neo4j.conf=/etc/neo4j/ \
etc/TCCDMDatum.avsc=/usr/local/include/tc_schema/
)

# Create db package for consumer dependency
fpm -s python -t deb .

popd
