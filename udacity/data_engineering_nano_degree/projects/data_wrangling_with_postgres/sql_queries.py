# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id CHAR(18),
start_time BIGINT,
user_id SMALLINT,
level CHAR(4),
song_id CHAR(18),
artist_id CHAR(18),
session_id SMALLINT,
location TEXT,
user_agent TEXT
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id SMALLINT PRIMARY KEY,
first_name TEXT,
last_name TEXT,
gender CHAR(1),
level CHAR(4)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id CHAR(18),
title TEXT,
artist_id CHAR(18),
year SMALLINT,
duration NUMERIC,
PRIMARY KEY(song_id, artist_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id CHAR(18) PRIMARY KEY,
name TEXT,
location TEXT,
latitude NUMERIC,
longitude NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time BIGINT,
hour SMALLINT,
day SMALLINT,
week  SMALLINT,
year SMALLINT,
weekday SMALLINT
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id,
                       start_time ,
                       user_id,
                       level,
                       song_id,
                       artist_id,
                       session_id,
                       location,
                       user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id,
                   first_name,
                   last_name,
                   gender,
                   level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO NOTHING
""")

song_table_insert = ("""
INSERT INTO songs (song_id,
                   title,
                   artist_id,
                   year,
                   duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id, artist_id)
DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,
                     name,
                     location,
                     latitude,
                     longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time (start_time,
                  hour,
                  day,
                  week,
                  year,
                  weekday)
VALUES (%s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, s.artist_id FROM artists a
JOIN songs s ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]