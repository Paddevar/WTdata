import yaml
import logging
import logging.config
from configparser import ConfigParser


config = ConfigParser()
config.read('config/params.conf')


def configure_logging():

    with open('config/logconfig.yml', 'r') as file:
        logconfig = yaml.safe_load(file)

    logging.config.dictConfig(logconfig)


def get_config_value(section: str, config_name: str):
    """Returns the specified value of the config variable. Falls back to the default value if not specified."""

    # TODO: ConfigParser has built-in functionality for default values, but the docs are very vague about it.
    value = config[section][config_name]
    value = config['DEFAULT'][config_name] if value is None else value

    return value

def get_kafka_servers() -> list[str]:

    # Use default server localhost:9092 if nothing else is configured
    kafka_servers = get_config_value('SERVERS', 'servers')

    # For consistency, always return a list, even if there is only one Kafka server configured.
    if kafka_servers is not list:
        kafka_servers = [kafka_servers]

    return kafka_servers


configure_logging()
kafka_servers = get_kafka_servers()
with open('config/queries.yml', 'r') as file:
    queries = yaml.safe_load(file)
