"""This module provides ETL pipeline for sparkifydb using postgres"""


import os
import glob
import logging
import psycopg2
import pandas as pd
from sql_queries import song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert, song_select


logger = logging.getLogger("etl")


def process_song_file(conn, filepath):
    """
    Get json file and insert data into song and artist tables
    param: conn: connection to database
    param: filepath: path to json file with song data
    return: None
    """
    cur = conn.cursor()

    df = pd.read_json(filepath, lines=True)

    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)

    artist_data = list(
        df[['artist_id',
            'artist_name',
            'artist_location',
            'artist_latitude',
            'artist_longitude']].values[0]
    )
    cur.execute(artist_table_insert, artist_data)


def process_log_file(conn, filepath):
    """
    Get log file and insert data into time, user and songplay tables
    param: conn: connection to database
    param: filepath: path to json file with log data
    return: None
    """

    cur = conn.cursor()
    df = pd.read_json(filepath, lines=True)
    df = df[df['page'] == 'NextSong']
    t = pd.to_datetime(df['ts'], unit='ms')
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month','year', 'weekday')
    time_df = pd.DataFrame(data=dict(zip(column_labels, time_data)))
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

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
    """
    Get all files in filepath and process them using func
    param: conn: connection to database
    param: filepath: path to the directory with data (json files)
    param: func: function to process data
    return: None
    """
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    logger.info('%s files found in %s', num_files, filepath)

    for i, datafile in enumerate(all_files, 1):
        func(conn, datafile)
        conn.commit()
        logger.info('%s/%s files processed.', i, num_files)


def setup_logging(level=logging.INFO):
    """
    Basic logger setup
    param: level: logging level
    return: None
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
