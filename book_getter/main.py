# Internal imports
import config.config as cfg
import open_library_scraper as ol
import google_scraper as gs
import kafka_publisher

# External imports
import time
import logging

logger = logging.getLogger(__name__)


def main():
    producer = kafka_publisher.get_producer()

    if cfg.populate:
        # Runs all queries once upon startup and publishes them to the configured kafka server(s).
        publish_queries(producer)

    if cfg.listen:
        # Listens for the newest book for the configured queries in a loop and publishes it,
        # until stopped manually.
        # TODO: only publish if the book has not been published to kafka yet! Currently there's no uniqueness check.
    
        listening_query = {'sort': 'new',
                           'limit': 1}
        while True:
            publish_queries(producer, listen=True)
            time.sleep(cfg.listen_rate)


def publish_queries(producer, listen= False):
    """Runs the queries configured in queries.yml, transforms the data
    and publishes it to kafka with one line per book. The configured queries can be overridden by the values in
    the passed dictionary."""

    # TODO: configure topics in query file? Allows for query-dependent topics.
    # topic = 'open_library_books'

    for site, site_queries in cfg.queries.items():

        # Each website has its own scraping functions.
        site_scraper = get_site_scraper(site)

        for site_query in site_queries.values():

            if listen:
                listen_query = get_listening_query(site)
                site_query = {**site_query, **listen_query}
            logger.info(f'Sending API call to {site} with {site_query=}.')

            df = site_scraper(site_query)
            logger.info(f'Received {len(df.rows())} valid books.')

            kafka_publisher.publish_df_rows(df, producer, cfg.topic)


def get_site_scraper(site: str) -> callable:
    """Returns the correct scraper for each site."""
    if site == 'OpenLibrary':
        return ol.open_library_books_to_df

    elif site == 'GoogleAPI':
        return gs.google_api_books_to_df
    
    # New websites can be added as follows:
    # elif site == 'NewWebsite':
    #     return SiteScraper

    else:
        raise NotImplementedError(f'Unknown website \"{site}\", check spelling in query configuration file.')


def get_listening_query(site):
    """Returns the query used to listen for new books."""
    # TODO: Should probably me merged with get_site_scraper into one function, e.g. get_site_info()
    if site == 'OpenLibrary':
        return {'sort': 'new',
                'limit': 1}

    elif site == 'GoogleAPI':
        return {'orderBy': 'newest',
                'maxResults': 1}

    else:
        raise NotImplementedError(f'Unknown website \"{site}\", check spelling in query configuration file.')

if __name__ == '__main__':
    main()
