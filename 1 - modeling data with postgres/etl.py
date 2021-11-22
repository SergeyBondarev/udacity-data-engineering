import os
import glob
import psycopg2
import logging
import pandas as pd
from sql_queries import *


logger = logging.getLogger("etl")


def process_song_file(conn, filepath):
    """Get json file and insert data into song and artist tables
    param: cur: cursor
    param: filepath: path to json file with song data
    """
    cur = conn.cursor()

    df = pd.read_json(filepath, lines=True)

    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates().values[0]
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.errors.UniqueViolation as e:
        conn.rollback()
        logger.info(f"Duplicate song_id found in {filepath}")
    else:
        conn.commit()

    artist_data = list(
        df[['artist_id',
            'artist_name',
            'artist_location',
            'artist_latitude',
            'artist_longitude']].drop_duplicates().values[0]
    )
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.errors.UniqueViolation:
        logger.warning(f"Duplicate artist_id found in {filepath}")
        conn.rollback()
    else:
        conn.commit()


def process_log_file(conn, filepath):
    """Get log file and insert data into time, user and songplay tables
    param: cur: cursor
    param: filepath: path to json file with log data
    """

    cur = conn.cursor()
    df = pd.read_json(filepath, lines=True)
    df = df[df['page'] == 'NextSong']
    t = pd.to_datetime(df['ts'], unit='ms')
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month','year', 'weekday')
    time_df = pd.DataFrame(data=dict(zip(column_labels, time_data)))

    for _, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.errors.UniqueViolation:
            logger.warning(f"Duplicate time_start {row[0]} found in {filepath}")
            conn.rollback()
        else:
            conn.commit()

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    for _, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.errors.UniqueViolation:
            logger.warning(f"Duplicate user_id {row[0]} found in {filepath}")
            conn.rollback()
        else:
            conn.commit()


    for _, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (row.ts, row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(conn, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def setup_logging(level=logging.ERROR):
    """
    Basic logger setup
    """
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=level)



def main():
    """
    Main function to connect to database and process data
    """
    setup_logging()

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")

    process_data(conn, filepath='data/song_data', func=process_song_file)
    process_data(conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
