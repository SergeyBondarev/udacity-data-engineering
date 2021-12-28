import logging

from sql_queries import create_table_queries, drop_table_queries
from util import connect_to_redshift, parse_config, setup_logger


logger = logging.getLogger(__name__)


def drop_tables(cur, conn):
    for query in drop_table_queries:
        logger.info(f'Executing query: {query}')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        logger.info(f'Executing query: {query}')
        cur.execute(query)
        conn.commit()

def main():
    config = parse_config()
    cur, conn = connect_to_redshift(config)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()