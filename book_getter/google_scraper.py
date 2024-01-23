# External imports
import requests
import json
import polars as pl
import logging

# Internal imports
import config.config as cfg

logger = logging.getLogger(__name__)

def google_api_books_to_df(query: dict) -> pl.DataFrame:
    """Main function of this module. Takes in a query dict, runs the query to the OpenLibrary API
    and turns it into a dataframe."""

    book_df = request_to_df(query)
    title_author_df = filter_book_df(book_df)

    return title_author_df


def search_books(query: dict) -> dict:
    """Runs a specific query to the OpenLibrary API and gets the results as a dict."""
    url = get_url(query)
    response_json = requests.get(url)
    response = json.loads(response_json.text)

    return response


def get_url(query: dict) -> str:
    """Construct the actual url for a specific query."""
    base_url = "https://www.googleapis.com/books/v1/volumes?q="
    query_url_part_one = '&'.join([f'{key}:{value}' for key, value in query.items()])
    query_url_part_two = '&startIndex=0&maxResults=5'
    key = '&key=AIzaSyAgjT2J6tpZMMkljqrNOKBnLQiFa55zGoQ'
    url = base_url + query_url_part_one + query_url_part_two + key

    return url


def request_to_df(query: dict) -> pl.DataFrame:
    """Turns the query result into a polars DataFrame."""
    book_results = search_books(query)
    data=[book_results['items'][i]['volumeInfo'] for i in range(len(book_results['items'])) ]
    thumbnail_data=[book_results['items'][i]['volumeInfo']['imageLinks']['thumbnail'] for i in range(len(book_results['items'])) ]
    isbn_data=[book_results['items'][i]['volumeInfo']['industryIdentifiers'][0]['identifier'] for i in range(len(book_results['items'])) ]
    source_data=['googleAPI'] * len(book_results['items'])
    base_book_df = pl.DataFrame(data=data)
    book_df=base_book_df.with_columns(pl.Series(name='thumbnail', values=thumbnail_data),pl.Series(name='isbnNumber', values=isbn_data),pl.Series(name='source', values=source_data))


    return book_df


def filter_book_df(book_df: pl.DataFrame) -> pl.DataFrame:
    """Filters out most of the returned book info and only leaves the essentials."""
    # TODO: also publish info such as isbn, descriptions etc.

    try:
        title_author_df = book_df.select(pl.col('title'),
                                         # Only pick out the first author.
                                         pl.col('authors').list.first().alias('author')
                                         ,pl.col('publisher').alias('publisher')
                                         ,pl.col('publishedDate').alias('releaseDate')
                                         ,pl.col('description').alias('description')
                                         ,pl.col('pageCount').alias('pageCount')
                                         ,pl.col('categories').alias('tag')
                                         ,pl.col('thumbnail').alias('imageUrl')
                                         ,pl.col('isbnNumber').alias('isbnNumber')
                                         ,pl.col('source').alias('source')
                                         ).drop_nulls()
    # If any of the required columns are not found in the query result, return an empty Dataframe.
    except pl.exceptions.ColumnNotFoundError:
        return pl.DataFrame()

    return title_author_df


def main():
    """For query testing purposes only."""
    query = {'subject': 'computers',
              #'intitle': 'test',
             'sort': 'new',
             # 'limit': 1
             }

    title_author_df = google_api_books_to_df(query)
 
    print(title_author_df)


if __name__ == '__main__':
    main()
