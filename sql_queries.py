import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar, 
    auth varchar, 
    firstName varchar, 
    gender varchar, 
    itemInSession int, 
    lastName varchar, 
    length DECIMAL, 
    level varchar, 
    location varchar, 
    method varchar, 
    page varchar, 
    registration varchar, 
    sessionId int, 
    song varchar, 
    status int, 
    ts BIGINT SORTKEY,
    userAgent varchar, 
    userId varchar)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id varchar SORTKEY,
    title varchar,
    artist_id varchar,
    artist_name varchar,
    artist_location varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    year int,
    num_songs int,
    duration float8)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays (
    songplay_id INT IDENTITY(0, 1) PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id varchar NOT NULL, 
    level varchar, 
    song_id varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    session_id int, 
    location varchar, 
    user_agent varchar,
    FOREIGN KEY(user_id) REFERENCES dim_users(user_id),
    FOREIGN KEY(song_id) REFERENCES dim_songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES dim_artists(artist_id),
    FOREIGN KEY(start_time) REFERENCES dim_time(start_time)
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users (
    user_id varchar PRIMARY KEY, 
    first_name varchar, 
    last_name varchar,
    gender varchar,
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar,
    year int NOT NULL,
    duration float8 NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id varchar PRIMARY KEY,
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time TIMESTAMP PRIMARY KEY,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
);
""")


###


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('S3', 'LOG_DATA'), config.get('DWH', 'DWH_ROLE_ARN'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('DWH', 'DWH_ROLE_ARN'))


# FINAL TABLES


songplay_table_insert = ("""
INSERT INTO fact_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + (ev.ts/1000) * INTERVAL '1 Second ' AS start_time
    ,ev.userId AS user_id
    ,ev.level AS level
    ,so.song_id AS song_id
    ,so.artist_id AS artist_id
    ,ev.sessionId AS session_id
    ,ev.location AS location
    ,ev.userAgent AS user_agent
FROM staging_events ev
JOIN staging_songs so ON (ev.song = so.title AND ev.artist = so.artist_name)
WHERE ev.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
    userId
    ,firstName
    ,lastName
    ,gender
    ,level
FROM staging_events ev
WHERE ev.page='NextSong'
AND userId is NOT null
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
    song_id
    ,title
    ,artist_id
    ,year
    ,duration 
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id
    ,artist_name
    ,artist_location
    ,artist_latitude
    ,artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
    TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 Second ' AS start_time,
    EXTRACT(HOUR FROM start_time),
    EXTRACT(DAY FROM start_time),
    EXTRACT(WEEK FROM start_time),
    EXTRACT(MONTH FROM start_time),
    EXTRACT(YEAR FROM start_time),
    EXTRACT(DOW FROM start_time)
FROM staging_events 
WHERE staging_events.page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert,songplay_table_insert]
