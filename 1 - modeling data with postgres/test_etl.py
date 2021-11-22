import pytest
import psycopg2
from create_tables import create_tables

from etl import process_song_file, process_log_file
from sql_queries import song_select


# CONNECTION_STRING = "host=127.0.0.1 dbname=sparkifydb_test user=test password=test"
CONNECTION_STRING = "host=127.0.0.1 user=test password=test"


@pytest.fixture(autouse=True)
def run_before_and_after_test():
    """Fixture to drop previous and create a new database"""
    cur, conn = connect(CONNECTION_STRING)

    drop_database(cur, "test_db")
    create_database(cur, "test_db")
    create_tables(cur, conn)

    conn.close()


def connect(connection_string):
    conn = psycopg2.connect(connection_string)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn


def drop_database(cur, db_name):
    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")


def create_database(cur, db_name):
    cur.execute(f"CREATE DATABASE {db_name} WITH ENCODING 'utf8' TEMPLATE template0")


def test_process_song_file():
    cur, conn = connect(CONNECTION_STRING)
    process_song_file(cur, "data/song_data/A/A/A/TRAAAAW128F429D538.json")
    cur.execute("SELECT * FROM songs WHERE artist_id = 'ARD7TVE1187B99BFB1'")
    result = cur.fetchone()
    assert result[0] == 'SOMZWCG12A8C13C480'
    assert result[1] == 'I Didn\'t Mean To' 
    assert result[3] == 0
    conn.close()


def test_process_log_file():
    cur, conn = connect(CONNECTION_STRING)
    process_log_file(cur, "data/log_data/2018/11/2018-11-01-events.json")
    cur.execute("SELECT * FROM users")
    result = cur.fetchone()
    assert result[0] == '8'
    assert result[1] == 'Kaylee'
    assert result[2] == 'Summers'
    assert result[3] == 'F'
    conn.close()
    
    