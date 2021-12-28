import logging
from sql_queries import copy_table_queries, insert_table_queries
from util import connect_to_redshift, parse_config, setup_logger


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


def main():
    setup_logger(logger)
    config = parse_config()

    cur, conn = connect_to_redshift(config)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
