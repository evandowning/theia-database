#!/bin/bash

fpm -s dir -t deb -n analysis_tools --config-files handler_database.cfg clear.sql create.sql neo4j.cron neo4j-load-csv.sh neo4j.conf \
                                           consume_database.py=/usr/bin/ \
                                           theia_anomaly.py=/usr/bin/ \
                                           theia_neo4j.py=/usr/bin/ \
                                           sensitive.py=/usr/bin/ \
                                           handler_database.cfg=/etc/theia/ \
                                           clear.sql=/etc/theia/ \
                                           create.sql=/etc/theia/ \
                                           neo4j.cron=/etc/theia/ \
                                           neo4j-load-csv.sh=/etc/theia/ \
                                           neo4j.conf=/etc/neo4j/
