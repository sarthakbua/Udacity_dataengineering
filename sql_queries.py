import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
#staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
#staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
#songplay_table_drop = "DROP table if exists songplays;"
#user_table_drop = "DROP TABLE if exists users;" 
#song_table_drop = "DROP TABLE if exists songs;"
#artist_table_drop = "DROP TABLE if exists artists;"
#time_table_drop = "DROP TABLE if exists time;"


# CREATE TABLES

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
artist        VARCHAR,
auth          VARCHAR,
firstName     VARCHAR,
gender        VARCHAR,
itemInSession INTEGER,
lastName      VARCHAR,
length        NUMERIC, 
level         VARCHAR,
location      VARCHAR,
method        VARCHAR,
page          VARCHAR,
registration   FLOAT,
sessionId     INTEGER,
song          VARCHAR,
status        VARCHAR,
ts            BIGINT,
userAgent     VARCHAR, 
userId        VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs       INTEGER,
artist_id       VARCHAR,
artist_latitude  FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name     VARCHAR,
song_id         VARCHAR,
title           VARCHAR,
duration         FLOAT,
year            INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL, 
user_id VARCHAR, 
level VARCHAR NOT NULL,
song_id VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
sessionId INTEGER,
artist_location VARCHAR,
userAgent VARCHAR 
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id VARCHAR PRIMARY KEY, 
firstName VARCHAR,
lastName VARCHAR, 
gender VARCHAR, 
level VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR NOT NULL,
year INTEGER,
duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id VARCHAR PRIMARY KEY,
artist_name VARCHAR,
artist_location VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time TIMESTAMP PRIMARY KEY, 
hour INTEGER, 
day VARCHAR, 
week INTEGER, 
month INTEGER, 
year INTEGER, 
weekday VARCHAR
);
""")
# STAGING TABLES

# COPY QUERIES:
staging_events_copy = """
 copy staging_events
    from {}
    iam_role {}
    json {}
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
copy {} from 's3://udacity-dend/{}' 
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2';
""").format('staging_songs','song_data',config.get("IAM_ROLE","ARN"))
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
#drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
