# External imports
import json
from kafka import KafkaProducer, KafkaConsumer
import logging

import config.config as cfg

logger = logging.getLogger(__name__)

def get_producer() -> KafkaProducer:

    kafka_servers = cfg.kafka_servers
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8')
                             )

    return producer


def get_consumer() -> KafkaConsumer:

    kafka_servers = get_kafka_servers()
    consumer = KafkaConsumer(bootstrap_servers=cfg.kafka_servers,
                             auto_offset_reset='earliest',
                             group_id='counters',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                             )

    return consumer



def publish_df_rows(df_to_publish, producer, topic) -> None:
    """Publish a dataframe row by row to the configured Kafka server and topic."""

    df_rows = df_to_publish.to_dicts()

    for df_row in df_rows:
        producer.send(topic=topic, value=df_row)
