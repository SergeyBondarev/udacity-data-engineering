import pytest
import psycopg2


@pytest.fixture(autouse=True)
def run_before_and_after_test():
    """Fixture to drop previous and create a new database"""
    pass


def connect(connection_string):
    conn = psycopg2.connect(connection_string)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn


def drop_database(cur, db_name):
    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")


def create_database(cur, db_name):
    cur.execute(f"CREATE DATABASE {db_name} WITH ENCODING 'utf8' TEMPLATE template0")
    