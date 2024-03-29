#!/usr/bin/env python2.7

import sys
import logging
import configparser
import time
import confluent_kafka

from consumer import TheiaConsumer
from theia_anomaly import TheiaAnomaly

log = logging.getLogger(__name__)
log_format = '[%(asctime)s] [%(levelname)s]: %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=log_format)

def usage():
    log.error('usage: python2.7 anomaly_detector.py handler_anomaly.cfg')
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

    # Create Anomaly Parser
    anomaly = None
    if config['anomaly'].getboolean('enable'):
        anomaly = TheiaAnomaly(config['anomaly'])

    i = 0

    # Consume CDM data
    try:
        while True:
            # See if data needs to be rotated/flushed
            if anomaly is not None:
                anomaly.flush()

            sys.stdout.write('Polling batch {0}...'.format(i))
            sys.stdout.flush()

            msgs = t_consumer.batch_consume(int(config['kafka']['batch_size']))
            d_msgs = t_consumer.batch_deserialize(msgs)

            sys.stdout.write('done\n')
            sys.stdout.flush()
            i += 1

            # For each CDM entry
            for m in d_msgs:
                if anomaly is not None:
                    anomaly.parse(m)

    finally:
        # Final rotate/flush
        if anomaly is not None:
            anomaly.final_flush()

    # Close Anomaly database connection
    if anomaly is not None:
        anomaly.shutdown()

if __name__ == '__main__':
    _main()
