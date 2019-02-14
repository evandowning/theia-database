import sys
import logging
import configparser
import time
import confluent_kafka

from consumer import TheiaConsumer
from theia_neo4j import TheiaNeo4j
from theia_anomaly import TheiaAnomaly

log = logging.getLogger(__name__)
log_format = '[%(asctime)s] [%(levelname)s]: %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=log_format)

def usage():
    log.error('usage: python2.7 consume_database.py handler_database.cfg')
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

    # Create Anomaly Parser
    anomaly = None
    if config['anomaly'].getboolean('enable'):
       anomaly = TheiaAnomaly(config['anomaly'])

    # Consume CDM data
    while True:
        # See if data needs to be rotated/flushed
        if neo4j is not None:
            neo4j.rotate()
        if anomaly is not None:
            anomaly.flush()

        msgs = t_consumer.batch_consume(int(config['kafka']['batch_size']))
        d_msgs = t_consumer.batch_deserialize(msgs)

        # For each CDM entry
        for m in d_msgs:
            if neo4j is not None:
                neo4j.parse(m)

            if anomaly is not None:
                anomaly.parse(m)

    # Final rotate/flush
    if neo4j is not None:
        neo4j.final_rotate()
    if anomaly is not None:
        anomaly.final_flush()

    # Close Anomaly database connection
    if anomaly is not None:
        anomaly.shutdown()

if __name__ == '__main__':
    _main()
