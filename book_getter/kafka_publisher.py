import json
from configparser import ConfigParser

from kafka import KafkaProducer, KafkaConsumer


def get_producer() -> KafkaProducer:

    kafka_servers = get_kafka_servers()
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8')
                             )

    return producer


def get_consumer() -> KafkaConsumer:

    kafka_servers = get_kafka_servers()
    consumer = KafkaConsumer(bootstrap_servers=kafka_servers,
                             auto_offset_reset='earliest',
                             group_id='counters',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                             )

    return consumer


def get_kafka_servers() -> list[str]:

    config = ConfigParser()
    config.read('kafka.conf')
    
    # Use default server localhost:9092 if nothing else is configured
    kafka_servers = config['SERVERS']['servers']
    kafka_servers = config['DEFAULT']['server'] if kafka_servers is None else kafka_servers

    # For consistency, always return a list, even if there is only one Kafka server configured.
    if kafka_servers is not list:
        kafka_servers = [kafka_servers]
    
    print(kafka_servers)
    
    return kafka_servers


def publish_df_rows(df_to_publish, producer, topic) -> None:
    """Publish a dataframe row by row to the configured Kafka server and topic."""

    df_rows = df_to_publish.to_dicts()
    
    counter = 0
    for df_row in df_rows:
        producer.send(topic=topic, value=df_row)
        counter +=1
    print(counter)
