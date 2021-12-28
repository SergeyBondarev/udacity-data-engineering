import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# TABLE NAMES
STAGING_EVENTS = "staging_events"
STAGING_SONGS = "staging_songs"
SONGPLAY = "songplay"
USER = "users"
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

staging_events_table_create= (f"""
    CREATE TABLE IF NOT EXISTS {STAGING_EVENTS} (
        artist VARCHAR(200),
        auth VARCHAR(20),
        firstName VARCHAR(50),
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR(50),
        length FLOAT,
        level VARCHAR(20),
        location VARCHAR(50),
        method VARCHAR(10),
        page VARCHAR(20),
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR(200),
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR(200),
        userId INTEGER
    );
""")

staging_songs_table_create = (f'''
    CREATE TABLE IF NOT EXISTS {STAGING_SONGS} (
        num_songs INTEGER,
        artist_id VARCHAR(20),
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR(200),
        artist_name VARCHAR(200),
        song_id VARCHAR(20),
        title VARCHAR(200),
        duration FLOAT,
        year INTEGER
    );
''')

songplay_table_create = (f'''
    CREATE TABLE IF NOT EXISTS {SONGPLAY} (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR(20),
        song_id VARCHAR(20),
        artist_id VARCHAR(20),
        session_id INTEGER,
        location VARCHAR(50),
        user_agent VARCHAR(200),
        FOREIGN KEY (song_id) REFERENCES {SONG} (song_id),
        FOREIGN KEY (artist_id) REFERENCES {ARTIST} (artist_id),
        FOREIGN KEY (user_id) REFERENCES {USER} (user_id),
        FOREIGN KEY (start_time) REFERENCES {TIME} (start_time)
    );
''')

user_table_create = (f'''
    CREATE TABLE IF NOT EXISTS {USER} (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(20),
        level VARCHAR(20)
    );
''')

song_table_create = (f'''
    CREATE TABLE IF NOT EXISTS {SONG} (
        song_id VARCHAR(20) PRIMARY KEY,
        title VARCHAR(200),
        artist_id VARCHAR(20),
        year INTEGER,
        duration FLOAT
    );
''')

artist_table_create = (f'''
    CREATE TABLE IF NOT EXISTS {ARTIST} (
        artist_id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(200),
        location TEXT,
        latitude FLOAT,
        longitude FLOAT
    );
''')

time_table_create = (f'''
    CREATE TABLE IF NOT EXISTS {TIME} (
        start_time timestamp PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );
''')

# STAGING TABLES

staging_events_copy = (f"""
    COPY {STAGING_EVENTS}
    from {config['S3']['LOG_DATA']}
    iam_role {config['IAM_ROLE']['ARN']}
    region 'us-west-2'
    json {config['S3']['LOG_JSONPATH']};
""")

staging_songs_copy = (f"""
    COPY {STAGING_SONGS}
    from {config['S3']['SONG_DATA']}
    iam_role {config['IAM_ROLE']['ARN']}
    region 'us-west-2'
    json 'auto';
""")

# FINAL TABLES

songplay_table_insert = (f'''
    INSERT INTO {SONGPLAY} (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT dateadd(second, ts, '1970-01-01 00:00:00') as start_time,  userId, level, song_id, artist_id, sessionId, location, userAgent
    FROM {STAGING_EVENTS}
    JOIN {STAGING_SONGS}
    ON {STAGING_EVENTS}.song = {STAGING_SONGS}.title
    WHERE userId IS NOT NULL;
''')

user_table_insert = (f'''
    INSERT INTO {USER} (user_id, first_name, last_name, gender, level)
    SELECT userId, firstName, lastName, gender, level
    FROM {STAGING_EVENTS}
    WHERE userId IS NOT NULL;
''')

song_table_insert = (f'''
    INSERT INTO {SONG} (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, EXTRACT(YEAR FROM (dateadd(second, ts, '1970-01-01 00:00:00'))), duration
    FROM {STAGING_EVENTS}
    JOIN {STAGING_SONGS}
    ON {STAGING_EVENTS}.song = {STAGING_SONGS}.title;
''')

artist_table_insert = (f'''
    INSERT INTO {ARTIST} (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM {STAGING_SONGS};
''')

time_table_insert = (f'''
    INSERT INTO {TIME} (start_time, hour, day, week, month, year, weekday)
    SELECT start_time, EXTRACT(HOUR FROM start_time), EXTRACT(DAY FROM start_time), EXTRACT(WEEK FROM start_time), EXTRACT(MONTH FROM start_time), EXTRACT(YEAR FROM start_time), EXTRACT(DOW FROM start_time)
    FROM {SONGPLAY};
''')

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
