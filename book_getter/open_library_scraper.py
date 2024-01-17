import requests
import json
import polars as pl


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


def request_to_df(query) -> pl.DataFrame:
    book_results = search_books(query)
    book_df = pl.DataFrame(book_results['docs'])

    return book_df


def filter_book_df(book_df) -> pl.DataFrame:

    try:
        title_author_df = book_df.select(pl.col('title'),
                                         pl.col('author_name').list.first().alias('author'),
                                         pl.col('subject').alias('tag')
                                         ).drop_nulls()
    except pl.exceptions.ColumnNotFoundError:
        return pl.DataFrame()

    return title_author_df


def open_library_books_to_df(query):
    book_df = request_to_df(query)
    title_author_df = filter_book_df(book_df)

    return title_author_df

def main():
    query = {'subject': 'computer',
             # 'title': 'test',
             'sort': 'new',
             # 'limit': 1
             }

    title_author_df = open_library_books_to_df(query)

    print(title_author_df)

if __name__ == '__main__':
    main()
