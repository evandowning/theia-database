import confluent_kafka
import sys
import logging
import uuid
import json

from tc.services import kafka
from tc.schema.serialization.kafka import KafkaAvroGenericDeserializer
from tc.schema.serialization.base import AvroJsonDeserializer
from tc.schema.serialization import Utils

log = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

class TheiaConsumer(object):
    """Consumes messages from kafka topic."""

    def __init__(self, conf, reset=False):
        """Set @reset to True to begin consuming at start of stream."""
        config = dict()
        self.topic = conf['kafka']['topic']
        config['bootstrap.servers'] = conf['kafka']['address']

        default_topic_config = {}
        default_topic_config["auto.offset.reset"] = "smallest"
        default_topic_config['enable.auto.commit'] = True
        config["default.topic.config"] = default_topic_config

        # Set the group ID.
        state = self._get_state_info(conf)
        if not reset and state:
            group_id = state['group_id']
        else:
            group_id = "CG_" + str(uuid.uuid4())
            self._update_state(conf,'group_id', group_id)
        config["group.id"] = group_id

        self.consumer = confluent_kafka.Consumer(config)
        self.consumer.subscribe([self.topic])

        schema = Utils.load_schema(conf['kafka']['schema'])
        c_schema = Utils.load_schema(conf['kafka']['schema'])
        self.deserializer = KafkaAvroGenericDeserializer(schema, schema)

    def batch_consume(self, count):
        msgs = list()
        kafka_msgs = self.consumer.consume(num_messages=count)
        msgs = [msg.value() for msg in kafka_msgs if not msg.error()]
        return msgs

    def batch_deserialize(self, msgs, include=None):
        """Deserializes @msgs. If @include is none, then deserializes all msgs.
        If @include is a list of strings, then only deserializes messages with
        contain this string."""

        d_msgs = list()
        for msg in msgs:
            if not include or any(x in msg for x in include):
                d_msg = self.deserializer.deserialize(self.topic, msg)
                d_msgs.append(d_msg)
        return d_msgs

    def _update_state(self, conf, key, value):
        state_file = conf['kafka']['kafka-state']
        with open(state_file, 'r') as outfile:
            state = json.load(outfile)
        with open(state_file, 'w') as outfile:
            state[key] = value
            json.dump(state, outfile)

    def _get_state_info(self, conf):
        try:
            with open(conf['kafka']['kafka-state'], 'r') as infile:
                return json.load(infile)
        except IOError:
            return self._init_state(conf)

    def _init_state(self, conf):
        with open(conf['kafka']['kafka-state'], 'w') as outfile:
            state = {'offset' : 0}
            json.dump(state, outfile)
        return None
