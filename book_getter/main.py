# External imports
import time
import yaml

# Internal imports
import open_library_scraper as ol
import kafka_publisher


def main():

    # Runs all queries once upon startup and publishes them to the configured kafka server(s).
    publish_queries()

    # Listens for the newest book for the configured queries in a loop and publishes it,
    # until stopped manually.
    # TODO: only publish if the book has not been published to kafka yet! Currently there's no uniqueness check.
    listening_query = {'sort': 'new',
                       'limit': 1}
    while True:
        publish_queries(override_query=listening_query)
        time.sleep(5)


def publish_queries(override_query=None):
    '''Runs the queries configured in queries.yml, transforms the data and publishes it to kafka with one line per book'''

    with open('queries.yml', 'r') as file:
        queries = yaml.safe_load(file)

    # TODO: configure topics in query file? Allows for query-dependent topics.
    topic = 'open_library_books'
    producer = kafka_publisher.get_producer()

    for site, site_queries in queries.items():

        # Each website has its own scraping functions.
        site_scraper = get_site_scraper(site)

        for site_query in site_queries.values():

            if override_query:
                site_query = {**site_query, **override_query}

            df = site_scraper(site_query)
            kafka_publisher.publish_df_rows(df, producer, topic)


def get_site_scraper(site: str) -> callable:
    
    """Returns the correct scraper for each site."""
    if site == 'OpenLibrary':
        return ol.open_library_books_to_df

    # New websites can be added as follows:
    # elif site == 'NewWebsite':
    #     return SiteScraper

    else:
        raise Exception(f'Unknown website \"{site}\", check spelling in query configuration file.')


if __name__ == '__main__':
    main()
