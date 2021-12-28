import logging
import configparser
import psycopg2


def setup_logger(logger):
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger.info('Logger is set up')


def connect_to_redshift(config):
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    return cur, conn


def parse_config():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config
