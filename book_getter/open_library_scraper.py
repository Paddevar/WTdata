import requests
import json
import polars as pl


def open_library_books_to_df(query: dict) -> pl.DataFrame:
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
    base_url = "https://openlibrary.org/search.json"
    query_url = '&'.join([f'{key}={value}' for key, value in query.items()])
    url = base_url + '?' + query_url

    return url


def request_to_df(query: dict) -> pl.DataFrame:
    """Turns the query result into a polars DataFrame."""
    book_results = search_books(query)
    book_df = pl.DataFrame(book_results['docs'])

    return book_df


def filter_book_df(book_df: pl.DataFrame) -> pl.DataFrame:
    """Filters out most of the returned book info and only leaves the essentials."""
    # TODO: also publish info such as isbn, descriptions etc.

    try:
        title_author_df = book_df.select(pl.col('title'),
                                         # Only pick out the first author.
                                         pl.col('author_name').list.first().alias('author'),
                                         pl.col('subject').alias('tag')
                                         ).drop_nulls()
    # If any of the required columns are not found in the query result, return an empty Dataframe.
    except pl.exceptions.ColumnNotFoundError:
        return pl.DataFrame()

    return title_author_df


def main():
    """For query testing purposes only."""
    query = {'subject': 'computer',
             # 'title': 'test',
             'sort': 'new',
             # 'limit': 1
             }

    title_author_df = open_library_books_to_df(query)

    print(title_author_df)


if __name__ == '__main__':
    main()
