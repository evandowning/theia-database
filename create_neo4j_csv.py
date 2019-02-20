#!/usr/bin/env python2.7

import sys
import logging
import configparser
import time
import confluent_kafka

from consumer import TheiaConsumer
from theia_neo4j import TheiaNeo4j

log = logging.getLogger(__name__)
log_format = '[%(asctime)s] [%(levelname)s]: %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=log_format)

def usage():
    log.error('usage: python2.7 create_neo4j_csv.py handler_neo4j.cfg')
    sys.exit(2)

def _main():
    if len(sys.argv) != 2:
        usage()

    configFN = sys.argv[1]

    # Read config file
    config = configparser.ConfigParser()
    config.read(configFN)

    # Create Consumer
    t_consumer = TheiaConsumer(config,config['kafka'].getboolean('reset'))

    # Create Neo4j Parser
    neo4j = None
    if config['neo4j'].getboolean('enable'):
       neo4j = TheiaNeo4j(config['neo4j'])

    # Consume CDM data
    while True:
        # See if data needs to be rotated/flushed
        if neo4j is not None:
            neo4j.rotate()

        print 'here'

        msgs = t_consumer.batch_consume(int(config['kafka']['batch_size']))
        d_msgs = t_consumer.batch_deserialize(msgs)

        # For each CDM entry
        for m in d_msgs:
            if neo4j is not None:
                neo4j.parse(m)

    # Final rotate/flush
    if neo4j is not None:
        neo4j.final_rotate()

if __name__ == '__main__':
    _main()
