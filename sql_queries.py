import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop =    "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =     "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =          "DROP TABLE IF EXISTS songplay "
user_table_drop =              "DROP TABLE IF EXISTS users"
song_table_drop =              "DROP TABLE IF EXISTS songs"
artist_table_drop =            "DROP TABLE IF EXISTS artists"
time_table_drop =              "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" 
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR(200),
    auth            VARCHAR(25),
    firstName       VARCHAR(25),
    gender          char,
    iteminSession   INTEGER,
    lastname        VARCHAR(25),
    lenght          DECIMAL(10, 5),
    level           VARCHAR(10),
    location        VARCHAR(300),
    method          VARCHAR(5),
    page            VARCHAR(50),
    registration    DECIMAL(15, 2),
    sessionid       INTEGER,
    song            VARCHAR(200),
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR(300),
    user_id          VARCHAR(20)

)
""")

staging_songs_table_create = (""" 
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id           VARCHAR(20),
    num_songs         INTEGER,
    artist_id         VARCHAR(20),
    artist_latitude   DECIMAL(10, 5),
    artist_longitude  DECIMAL(10, 5),
    artist_location   VARCHAR(300),
    artist_name       VARCHAR(200),
    song_title        VARCHAR(200),
    duration          DECIMAL(10, 5),
    year              INTEGER

)
""")

songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplay(
    songplay_id        INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time         TIMESTAMP NOT NULL,
    user_id            VARCHAR(20),
    level              VARCHAR(10),
    song_id            VARCHAR(20),
    artist_id          VARCHAR(20),
    session_id         INTEGER,
    artist_location    VARCHAR(300),
    user_agent         VARCHAR(300)

)
""")

user_table_create = (""" 
CREATE TABLE IF NOT EXISTS users (
    user_id     VARCHAR(20)  PRIMARY KEY,
    firstname  VARCHAR(25),
    lastname   VARCHAR(25),
    gender      CHAR,
    level       VARCHAR(10)
)
""")

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR(20)  PRIMARY KEY,
    song_title  VARCHAR(200),
    artist_id   VARCHAR(20),
    year        INTEGER,
    duration    DECIMAL(10, 5)
)
""")

artist_table_create = (""" 
CREATE TABLE IF NOT EXISTS artists (
    artist_id          VARCHAR(20)  PRIMARY KEY,
    artist_name        VARCHAR(200) NOT NULL,
    artist_location    VARCHAR(300),
    artist_latitude   DECIMAL(10, 5),
    artist_longitude   DECIMAL(10, 5)
)
""")

time_table_create = (""" 
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP  PRIMARY KEY,
    hour    INTEGER,
    day     INTEGER,
    week    INTEGER,
    month   INTEGER,
    year    INTEGER,
    weekday INTEGER
)
""")

# STAGING TABLES

staging_events_copy = (""" 
COPY staging_events FROM {}
    iam_role {}
    FORMAT AS JSON {}
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (""" 
COPY staging_songs FROM {}
    iam_role {}
    FORMAT AS JSON 'auto'
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = (""" 
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, artist_location, user_agent) 

SELECT 
     TIMESTAMP WITHOUT TIME ZONE 'epoch' + (se.ts::float / 1000) * INTERVAL '1 second',
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionid,
    se.location,
    se.userAgent
FROM staging_events se
INNER JOIN staging_songs ss 
    ON se.song = ss.song_title and se.artist = ss.artist_name
WHERE page = 'NextSong'
""")

user_table_insert = (""" INSERT INTO users (user_id, firstname, lastname, gender, level)
SELECT 
    se.user_id,
    se.firstName,
    se.lastname,
    se.gender,
    se.level
FROM staging_events se
""")


song_table_insert = (""" INSERT INTO songs (song_id, song_title, artist_id, year, duration)
SELECT 
    song_id,
    song_title,
    artist_id,
    CASE WHEN year != 0 THEN year ELSE null END AS year,
    duration
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists ( artist_id,artist_name, artist_location, artist_latitude, artist_longitude)

SELECT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude

FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time  (start_time,hour, day, week, month, year, weekday)
SELECT 
    start_time,
    EXTRACT(HOUR FROM start_time),
    EXTRACT(DAY FROM start_time),
    EXTRACT(WEEK FROM start_time),
    EXTRACT(MONTH FROM start_time),
    EXTRACT(YEAR FROM start_time),
    EXTRACT(DOW FROM start_time)
FROM (SELECT DISTINCT start_time FROM songplay)
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create,  staging_events_table_create, staging_songs_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [ user_table_insert,songplay_table_insert, song_table_insert, artist_table_insert, time_table_insert]
