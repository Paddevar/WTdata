import requests
import json
import itertools as it
import polars as pl
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
    # Supports specifying multiple hosts and ports. All host:port combinations will be used.
    kafka_hosts = ['localhost']
    kafka_ports = ['9092']
    kafka_servers = [':'.join(host_and_port) for host_and_port in it.product(kafka_hosts, kafka_ports)]

    return kafka_servers


def request_to_df(query) -> pl.DataFrame:
    book_results = search_books(query)
    book_df = pl.DataFrame(book_results['docs'])

    return book_df


def filter_book_df(book_df) -> pl.DataFrame:
    title_author_df = book_df.select(pl.col('title'),
                                     pl.col('author_name').list.first().alias('author'),
                                     pl.col('subject').alias('tag')
                                     ).drop_nulls()

    return title_author_df


def publish_to_kafka(info, topic='open_library_books'):
    kafka_servers = get_kafka_servers()
    producer = get_producer(kafka_servers)
    consumer = get_consumer(kafka_servers)

    producer.send(topic=topic, value=info)


def main():
    query = {'subject': 'computer',
             # 'title': 'test',
             'sort': 'new',
             # 'limit': 1
             }

    book_df = request_to_df(query)
    title_author_df = filter_book_df(book_df)

    info_to_publish = title_author_df.to_dicts()

    for info in info_to_publish:
        publish_to_kafka(info)

    # while True:
    #
    #     latest_book = books['docs'][0]
    #
    #     if latest_book != new_book:
    #         new_book = latest_book
    #         producer.send(topic=topic, value=new_book)
    #
    #     time.sleep(5)


if __name__ == '__main__':
    main()
