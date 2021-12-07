import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# TABLE NAMES
STAGING_EVENTS = "staging_events"
STAGING_SONGS = "staging_songs"
SONGPLAY = "songplay"
USER = "user"
SONG = "song"
ARTIST = "artist"
TIME = "time"

# DROP TABLES

staging_events_table_drop = f"DROP TABLE IF EXISTS {STAGING_EVENTS}"
staging_songs_table_drop = f"DROP TABLE IF EXISTS {STAGING_SONGS}"
songplay_table_drop = f"DROP TABLE IF EXISTS {SONGPLAY}"
user_table_drop = f"DROP TABLE IF EXISTS {USER}"
song_table_drop = f"DROP TABLE IF EXISTS {SONG}"
artist_table_drop = f"DROP TABLE IF EXISTS {ARTIST}"
time_table_drop = f"DROP TABLE IF EXISTS {TIME}"

# CREATE TABLES

staging_events_table_create= ("""
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ('''
    CREATE TABLE IF NOT EXISTS {SONGPLAY} (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id int INTEGER NOT NULL,
        level VARCHAR(20),
        song_id VARCHAR(20),
        artist_id VARCHAR(20),
        session_id INT,
        location VARCHAR(50),
        user_agent VARCHAR(200),
        FOREIGN KEY (song_id) REFERENCES song (song_id),
        FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
        FOREIGN KEY (user_id) REFERENCES user (user_id),
        FOREIGN KEY (start_time) REFERENCES time (start_time)
    )
''')

user_table_create = (f'''
    CREATE TABLE IF NOT EXISTS {USER} (
        user_id int PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(20),
        level VARCHAR(20)
    )
''')

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS 
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
