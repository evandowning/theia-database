#!/bin/bash

fpm -s dir -t deb -n analysis_tools --config-files handler_neo4j.cfg handler_anomaly.cfg clear.sql create.sql neo4j.cron neo4j-load-csv.sh neo4j.conf \
                                           create_neo4j_csv.py=/usr/bin/ \
                                           anomaly_detector.py=/usr/bin/ \
                                           theia_anomaly.py=/usr/bin/ \
                                           theia_neo4j.py=/usr/bin/ \
                                           sensitive.py=/usr/bin/ \
                                           handler_neo4j.cfg=/etc/theia/ \
                                           handler_anomaly.cfg=/etc/theia/ \
                                           create.sql=/etc/theia/ \
                                           clear.sql=/etc/theia/ \
                                           neo4j.cron=/etc/theia/ \
                                           neo4j-load-csv.sh=/etc/theia/ \
                                           neo4j.conf=/etc/neo4j/
