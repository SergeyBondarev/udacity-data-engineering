"""
This module contains analytical queries.
"""

import logging
from argparse import ArgumentParser

from util import setup_logger, parse_config, connect_to_redshift


logger = logging.getLogger(__name__)


def most_active_users(cur, conn, k=10):
    """
    This function returns the top-k most active users.
    """
    query = f"""
        SELECT first_name, last_name, COUNT(songplay.user_id) AS count
        FROM songplay
        JOIN users
        ON songplay.user_id = users.user_id
        GROUP BY songplay.user_id, first_name, last_name
        ORDER BY count DESC
        LIMIT {k};
    """
    logger.info('Executing query: %s', query)
    cur.execute(query)
    results = cur.fetchall()
    conn.commit()
    return results


def most_popular_songs(cur, conn, k=10):
    """
    This function returns the top-k most popular songs.
    """
    query = f"""
        SELECT title, COUNT(songplay.song_id) AS count
        FROM songplay
        JOIN song
        ON songplay.song_id = song.song_id
        GROUP BY songplay.song_id, title
        ORDER BY count DESC
        LIMIT {k};
    """
    logger.info('Executing query: %s', query)
    cur.execute(query)
    results = cur.fetchall()
    conn.commit()
    return results


def most_popular_artists(cur, conn, k=10):
    """
    This function returns the top-k most popular artists.
    """
    query = f"""
        SELECT name, COUNT(songplay.artist_id) AS count
        FROM songplay
        JOIN artist
        ON songplay.artist_id = artist.artist_id
        GROUP BY songplay.artist_id, name
        ORDER BY count DESC
        LIMIT {k};
    """
    logger.info('Executing query: %s', query)
    cur.execute(query)
    results = cur.fetchall()
    conn.commit()
    return results


def setup_parser():
    """
    This function sets up the argument parser.
    """
    parser = ArgumentParser(
        description='Analytics queries functionality',
    )

    root_parser = parser.add_subparsers(
        dest='command',
    )

    most_active_users_parser = root_parser.add_parser(
        'most_active_users',
        help='Returns the top-k most active users',
    )
    most_active_users_parser.add_argument(
        '-k',
        type=int,
        default=10,
        help='Number of users to return',
    )


    most_popular_songs_parser = root_parser.add_parser(
        'most_popular_songs',
        help='Returns the top-k most popular songs',
    )
    most_popular_songs_parser.add_argument(
        '-k',
        type=int,
        default=10,
        help='Number of songs to return',
    )


    most_popular_artists_parser = root_parser.add_parser(
        'most_popular_artists',
        help='Returns the top-k most popular artists',
    )
    most_popular_artists_parser.add_argument(
        '-k',
        type=int,
        default=10,
        help='Number of artists to return',
    )

    return parser


def parse_args():
    """
    This function parses the arguments.
    """
    parser = setup_parser()
    args = parser.parse_args()
    
    return args


def execute_query(cur, conn, args):
    """
    This function executes a query depending on the args.
    """
    if args.command == 'most_active_users':
        results = most_active_users(cur, conn, args.k)
    elif args.command == 'most_popular_songs':
        results = most_popular_songs(cur, conn, args.k)
    elif args.command == 'most_popular_artists':
        results = most_popular_artists(cur, conn, args.k)
    else:
        raise ValueError('Invalid query')
    return results


def main():
    """
    Main function of the module.
    """
    setup_logger(logger)
    config = parse_config()
    cur, conn = connect_to_redshift(config)

    args = parse_args()
    results = execute_query(cur, conn, args)
    for res in results:
        print(res)

    conn.close()


if __name__ == "__main__":
    main()
