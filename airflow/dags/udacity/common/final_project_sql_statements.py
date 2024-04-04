class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
     SELECT distinct start_time,
    EXTRACT(HOUR FROM start_time) AS HOUR,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEK FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    EXTRACT(DOW FROM start_time) AS weekday
FROM (
SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time
FROM staging_events)
    """)

staging_events=("""
        create TABLE IF NOT EXISTS staging_events (
        artist        VARCHAR(MAX),
        auth          VARCHAR(MAX),
        firstName     VARCHAR(MAX), 
        gender        VARCHAR(MAX),
        itemInSession INTEGER,
        lastName      VARCHAR(MAX),
        length        FLOAT,
        level         VARCHAR(MAX),
        location      VARCHAR(MAX),
        method        VARCHAR(MAX),
        page          VARCHAR(MAX),
        registration  BIGINT,
        sessionId     INTEGER,
        song          VARCHAR(MAX),
        status        INTEGER, 
        ts            BIGINT, 
        userAgent     VARCHAR(MAX),
        userid        INTEGER
);

""")

staging_songs=( """
    CREATE TABLE IF NOT EXISTS staging_songs(
    song_id          VARCHAR,     
    num_songs        INTEGER,      
    title            VARCHAR(MAX),    
    artist_name      VARCHAR(MAX),  
    artist_latitude  FLOAT,             
    year             INTEGER,            
    duration         FLOAT,         
    artist_id        VARCHAR(MAX),  
    artist_longitude FLOAT,           
    artist_location  VARCHAR(MAX)      
);
""")

songplay_table_create= ("""
CREATE TABLE IF NOT EXISTS songplay_fact_table(
songplay_id    VARCHAR           NOT NULL  PRIMARY KEY,
start_time     BIGINT            NOT NULL,
user_id        INTEGER           NOT NULL,
level          VARCHAR           NOT NULL,
song_id        VARCHAR           NOT NULL,
artist_id      VARCHAR(50)       NOT NULL,
session_id     INTEGER           NOT NULL,
location       VARCHAR(MAX)          NULL, 
user_agent     VARCHAR(MAX)      NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE if not exists user_dim_table(
user_id    INTEGER         NOT NULL  PRIMARY KEY,
first_name VARCHAR(50)     NOT NULL,
last_name  VARCHAR(50)     NOT NULL,
gender     VARCHAR(5)      NOT NULL,
level      VARCHAR         NOT NULL
);
""")


song_table_create = ("""
CREATE TABLE if not exists song_dim_table(
song_id   VARCHAR    NOT NULL   PRIMARY KEY,
title     VARCHAR    NOT NULL,
artist_id VARCHAR    NOT NULL,
year      INTEGER    NOT NULL,
duration  INTEGER    NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE if not exists artist_dim_table (
artist_id   VARCHAR         NOT NULL  PRIMARY KEY,
name        VARCHAR(max)    NOT NULL,
location    VARCHAR(MAX)        NULL,
latitude    FLOAT               NULL,
longitude   FLOAT               NULL
);
""")


time_table_create = ("""
CREATE TABLE if not exists time_dim_table (
start_time timestamp   NOT NULL   PRIMARY KEY,
hour    INTEGER        NOT NULL,
day     INTEGER        NOT NULL,
week    INTEGER        NOT NULL,
month   INTEGER        NOT NULL,
year    INTEGER        NOT NULL,
weekday INTEGER        NOT NULL
);
""")
