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
    url = GoogleURL(query).construct_url()
    response_json = requests.get(url)

    response = json.loads(response_json.text)

    return response

class GoogleURL:

    def __init__(self, query):
        self.query = query
        self.base_url = "https://www.googleapis.com/books/v1/volumes?"
        self.search_fields = ['intitle', 'inauthor', 'inpublisher', 'subject', 'isbn', 'lccn', 'oclc']


    def construct_url(self) -> str:
        query_url = self.construct_query_url()
        url = (self.base_url + query_url)

        logger.info(f'The query url is {url}')

        return url

    def construct_query_url(self) -> str:

        fulltext_query_url = "q=" + self.query['q'].replace(" ", "+")
        infields_query_url = "+" + "&".join([f'{key}:{value}' for key, value in self.query.items()
                                             if key in self.search_fields])
        special_query_url = "&" + "&".join([f'{key}={value}' for key, value in self.query.items()
                                            if key not in self.search_fields + ['q']])

        return fulltext_query_url + infields_query_url + special_query_url

    def key_separator(self, key: str) -> str:

        if key in self.search_fields:
            return ":"
        else:
            return "="


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
                                         ).drop_nulls(subset='title').drop_nulls(subset='author')
        
    # If any of the required columns are not found in the query result, return an empty Dataframe.
    except pl.exceptions.ColumnNotFoundError:
        return pl.DataFrame()

    return title_author_df


def main():
    """For query testing purposes only."""

    query = {'q': 'python',
             'intitle': 'programming',
             # 'title': 'test',
             'orderBy': 'newest',
             "startIndex": 0,
             "maxResults": 1

             }

    google_url = GoogleURL(query).construct_url()
    # print(google_url)
    # google_url_constructor(query)
    # title_author_df = google_api_books_to_df(query)

    # print(title_author_df)


if __name__ == '__main__':
    main()
