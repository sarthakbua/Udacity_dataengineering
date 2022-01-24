import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

songplay_table_drop = "DROP table if exists songplays;"
user_table_drop = "DROP TABLE if exists users" 
song_table_drop = "DROP TABLE if exists songs"
artist_table_drop = "DROP TABLE if exists artists"
time_table_drop = "DROP TABLE if exists time"
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    songs_id SERIAL,
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id varchar, 
location varchar, 
user_agent varchar)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id int distkey,
first_name varchar ,
last_name varchar ,
gender varchar ,
level varchar ,
PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR NOT NULL,
year INTEGER,
duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar sortkey,
name varchar NOT NULL,
location varchar ,
latitude decimal ,
logitude decimal ,
PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time timestamp sortkey,
hour int NOT NULL,
day int NOT NULL,
week int NOT NULL,
month int NOT NULL,
year int NOT NULL,
weekday int NOT NULL,
PRIMARY KEY (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {}
REGION 'us-west-2'
""").format(
        config.get("S3", "LOG_DATA"),
        config.get("IAM_ROLE", "ARN"),
        config.get("S3", "LOG_JSONPATH")
        )

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2'
    compupdate off
""").format(
        config.get("S3", "SONG_DATA"),
        config.get("IAM_ROLE", "ARN")
            )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, sessionId, artist_location, userAgent)
SELECT TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' as start_time,
       se.userId,
       se.level,
       ss.song_id,
       ss.artist_id,
       se.sessionId,
       se.location,
       se.userAgent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, firstName, lastName, gender, level)
SELECT DISTINCT se.userId,
       se.firstName,
       se.lastName,
       se.gender,
       se.level
FROM staging_events se
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT ss.song_id,
       ss.title,
       ss.artist_id,
       ss.year,
       ss.duration
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT ss.artist_id,
       ss.artist_name,
       ss.artist_location,
       ss.artist_latitude,
       ss.artist_longitude
FROM staging_songs ss
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEK FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       EXTRACT(DOW FROM start_time) AS weekday
FROM staging_events se
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
