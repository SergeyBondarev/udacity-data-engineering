"""
This module is used for ETLing the data from S3 to Redshift.
"""


import logging
from sql_queries import copy_table_queries, insert_table_queries
from util import connect_to_redshift, parse_config, setup_logger


logger = logging.getLogger(__name__)


def load_staging_tables(cur, conn):
    """
    This function loads data from S3 to staging tables.
    """
    for query in copy_table_queries:
        logger.info('Executing query: %s', query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts data from staging tables to dimensional and fact tables.
    """
    for query in insert_table_queries:
        logger.info('Executing query: %s', query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function of the ETL process.
    """
    setup_logger(logger)
    config = parse_config()

    cur, conn = connect_to_redshift(config)

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
