import time
import requests
import json
import itertools as it
from kafka import KafkaProducer, KafkaConsumer


def search_books(query: dict):

    url = get_url(query)
    response_json = requests.get(url)
    response_json = json.loads(response_json.text)

    return response_json


def get_url(query: dict) -> str:

    base_url = "https://openlibrary.org/search.json"
    query_url = '&'.join([f'{key}={value}' for key, value in query.items()])
    url = base_url + '?' + query_url

    return url


def get_producer(kafka_servers: list[str]) -> KafkaProducer:

    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8')
                             )

    return producer


def get_consumer(kafka_servers: list[str]) -> KafkaConsumer:

    consumer = KafkaConsumer(bootstrap_servers=kafka_servers,
                             auto_offset_reset='earliest',
                             group_id='counters',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                             )

    return consumer


def get_kafka_servers() -> list[str]:

    kafka_hosts = ['localhost']
    kafka_ports = ['9092']
    kafka_servers = [':'.join(host_and_port) for host_and_port in it.product(kafka_hosts, kafka_ports)]

    return kafka_servers


def main():

    kafka_servers = get_kafka_servers()
    producer = get_producer(kafka_servers)
    consumer = get_consumer(kafka_servers)

    query = {'q': 'programming',
             # 'title': 'test',
             'sort': 'new',
             'limit': 1
             }

    new_book = {}
    topic = 'open_library_books'

    while True:

        books = search_books(query)
        latest_book = books['docs'][0]

        if latest_book != new_book:
            new_book = latest_book
            producer.send(topic=topic, value=new_book)

        time.sleep(5)


if __name__ == '__main__':
    main()
