import configparser
import logging
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


logger = logging.getLogger(__name__)


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        logger.info(f'Executing query: {query}')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        logger.info(f'Executing query: {query}')
        cur.execute(query)
        conn.commit()


def connect_to_redshift(config):
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    return cur, conn


def parse_config():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config


def setup_logger(config_filepath='logging.yaml', level=logging.INFO):
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger.info('Logger is set up')


def main():
    setup_logger()
    config = parse_config()

    cur, conn = connect_to_redshift(config)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
