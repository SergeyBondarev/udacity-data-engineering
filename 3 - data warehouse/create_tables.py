"""
This module drops / re-creates tables in Redshift.
"""


import logging

from sql_queries import create_table_queries, drop_table_queries
from util import connect_to_redshift, parse_config, setup_logger


logger = logging.getLogger(__name__)


def drop_tables(cur, conn):
    """
    This function drops tables in Redshift.
    """
    for query in drop_table_queries:
        logger.info('Executing query: %s', query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates tables in Redshift.
    """
    for query in create_table_queries:
        logger.info('Executing query: %s', query)
        cur.execute(query)
        conn.commit()


def main():
    """
    This function drops and recreates tables in Redshift.
    """
    setup_logger(logger)
    config = parse_config()
    cur, conn = connect_to_redshift(config)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
