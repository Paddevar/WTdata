import yaml
import logging
import logging.config
from configparser import ConfigParser
from pathlib import Path
from dataclasses import dataclass

# Define config file locations
config_dir = Path(__file__).parent
param_file = config_dir / 'params.conf'
queries_file = config_dir / 'queries.yml'
logconfig_file = config_dir / 'logconfig.yml'

# Load queries
with open(queries_file, 'r') as file:
    queries = yaml.safe_load(file)


def configure_logging():
    """Load log_config file and apply its settings."""

    with open(logconfig_file, 'r') as file:
        logconfig = yaml.safe_load(file)

    logging.config.dictConfig(logconfig)


configure_logging()

# Load global parameters and use them to set the kafka bootstrap-servers.
config = ConfigParser()
config.read(param_file)


def get_kafka_servers() -> list[str]:
    # Use default server localhost:9092 if nothing else is configured
    kafka_servers = get_config_value('SERVERS', 'servers')

    # For consistency, always return a list, even if there is only one Kafka server configured.
    if kafka_servers is not list:
        kafka_servers = [kafka_servers]

    return kafka_servers


def get_config_value(section: str, config_name: str):
    """Returns the specified value of the config variable. Falls back to the default value if not specified."""

    # TODO: ConfigParser has built-in functionality for default values, but the docs are very vague about it.
    value = config[section][config_name]
    value = config['DEFAULT'][config_name] if value is None else value

    return value


kafka_servers = get_kafka_servers()
listen_rate = int(get_config_value('MODE', 'listen_rate'))
populate = config.getboolean('MODE', 'populate')
listen = config.getboolean('MODE', 'listen')
topic = get_config_value('SERVERS', 'topic')
