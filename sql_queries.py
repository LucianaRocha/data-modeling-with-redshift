# Import the necessary packages
import configparser

# Set the dwh.cfg with necessary information
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

# Create database schema
create_schema = "CREATE SCHEMA IF NOT EXISTS dwh_redshift;"

# Set the search_path with the created database schema
set_path_dwh = "SET search_path TO dwh_redshift;"

# Create a drop table clause for each table
staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplay CASCADE;"
users_table_drop = "DROP table IF EXISTS users CASCADE;"
song_table_drop = "DROP table IF EXISTS song CASCADE;"
artist_table_drop = "DROP table IF EXISTS artist CASCADE;"
time_table_drop = "DROP table IF EXISTS times CASCADE;"

# Create a create table clause for each table
staging_events_table_create = (
    "CREATE table IF NOT EXISTS staging_events (" \
        "artist varchar," \
        "auth varchar not null," \
        "firstName varchar," \
        "gender char (1)," \
        "itemInSession int not null," \
        "lastName varchar," \
        "length numeric," \
        "level varchar not null," \
        "location varchar," \
        "method varchar not null," \
        "page varchar not null," \
        "registration numeric," \
        "sessionId int not null," \
        "song varchar," \
        "status int not null," \
        "ts numeric not null," \
        "userAgent varchar," \
        "userId int" \
        ")"
)

staging_songs_table_create = (
    "CREATE table IF NOT EXISTS staging_songs (" \
        "num_songs int not null," \
        "artist_id char (18) not null," \
        "artist_latitude varchar," \
        "artist_longitude varchar," \
        "artist_location varchar," \
        "artist_name varchar not null," \
        "song_id char (18) not null," \
        "title varchar not null," \
        "duration numeric not null," \
        "year int not null" \
        ")"
)

songplay_table_create = (
    "CREATE table IF NOT EXISTS songplays (" \
        "songplay_id int identity(0, 1)," \
        "start_time TIMESTAMP sortkey distkey," \
        "user_id int," \
        "level varchar," \
        "song_id varchar," \
        "artist_id varchar," \
        "session_id int," \
        "location varchar," \
        "user_agent varchar, " \
        "PRIMARY KEY (songplay_id)," \
        "FOREIGN KEY (start_time) REFERENCES times (start_time)," \
        "FOREIGN KEY (user_id) REFERENCES users (user_id)," \
        "FOREIGN KEY (song_id) REFERENCES songs (song_id)," \
        "FOREIGN KEY (artist_id) REFERENCES artists (artist_id)" \
        ")"
    )

users_table_create = (
    "CREATE table IF NOT EXISTS users (" \
        "user_id int sortkey," \
        "first_name varchar," \
        "last_name varchar," \
        "gender varchar," \
        "level varchar," \
        "PRIMARY KEY (user_id)" \
        ") " \
        "diststyle all;"
    )

song_table_create = (
    "CREATE table IF NOT EXISTS songs (" \
        "song_id varchar NOT NULL sortkey distkey," \
        "title varchar," \
        "artist_id varchar NOT NULL," \
        "year int," \
        "duration numeric," \
        "PRIMARY KEY (song_id)" \
        ")"
    )

artist_table_create = (
    "CREATE table IF NOT EXISTS artists (" \
        "artist_id varchar NOT NULL sortkey distkey," \
        "name varchar," \
        "location varchar," \
        "latitude numeric," \
        "longitude numeric," \
        "PRIMARY KEY (artist_id) " \
        ") "
    )

time_table_create = (
    "CREATE table IF NOT EXISTS times (" \
        "start_time TIMESTAMP sortkey," \
        "hour int," \
        "day int," \
        "week int," \
        "month int," \
        "year int," \
        "weekday int," \
        "PRIMARY KEY (start_time)" \
        ") " \
        "diststyle all;"
    )

# Create a copy clause for each file
staging_events_copy = (
    "copy staging_events from {} " \
    "iam_role {} " \
    "format as json {} " \
).format(
    config.get("S3", "LOG_DATA"),
    ARN,
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = (
    "copy staging_songs from {} " \
    "iam_role {} " \
    "json 'auto' " \
).format(config.get("S3", "SONG_DATA"), ARN)

# Create a insert table clause for each table
user_table_insert = (
    "INSERT INTO users " \
        "SELECT userid, firstname, lastname, gender, level " \
        "FROM " \
            "( "
            "SELECT MAX(ts) as ts, userid, firstname, "\
  		    "lastname, gender, level "\
            "FROM staging_events "\
            "WHERE page = 'NextSong'  "\
            "GROUP BY  "\
            "userid, firstname, lastname, gender, level "\
            "ORDER BY userid ASC "\
            ")"
)

song_table_insert = (
    "INSERT INTO songs " \
        "SELECT song_id, title, artist_id, year, duration " \
        "FROM ( " \
        "SELECT MAX(year) AS year, song_id, title, artist_id, " \
        "duration " \
        "FROM staging_songs " \
        "GROUP BY song_id, title, artist_id, duration " \
        ")"
)

artist_table_insert = (
        "INSERT INTO artists " \
            "SELECT DISTINCT artist_id, artist_name, " \
            "artist_location, artist_latitude, artist_longitude " \
            "FROM staging_songs"
)

time_table_insert = (
        "INSERT INTO times " \
            "SELECT " \
            "start_time, " \
            "extract(hour from start_time) as hour, " \
            "extract(day from start_time) as day, " \
            "extract(week from start_time) as week, " \
            "extract(month from start_time) as month, " \
            "extract(year from start_time) as year, " \
            "extract(weekday from start_time) as weekday " \
            "FROM ( " \
            "    SELECT DISTINCT " \
            "    timestamp 'epoch' + ts / 1000 * interval '1 second'" \
            " as start_time " \
            "    FROM staging_events " \
            "    WHERE page = 'NextSong' " \
            ") time"
)

songplay_table_insert = (
    "INSERT INTO songplays (" \
            "start_time, user_id, level, song_id, artist_id, " \
            "session_id, location, user_agent" \
            ")" \
        "SELECT " \
        "timestamp 'epoch' + events.ts / 1000 * interval '1 second' " \
        "as start_time, " \
        "events.userId as user_id, " \
        "events.level, " \
        "songs.song_id, " \
        "songs.artist_id, " \
        "events.sessionId as session_id, " \
        "events.location, " \
        "events.userAgent as user_agent " \
        "FROM staging_events events " \
        "LEFT JOIN staging_songs songs ON " \
        "events.song = songs.title  " \
        "AND events.artist = songs.artist_name " \
        "WHERE events.page = 'NextSong' "
)

# Create query lists
create_schema_redshift = [
    create_schema
    ]

set_path_dwh = [
    set_path_dwh
    ]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    users_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
    ]

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    users_table_create, 
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
    ]

copy_table_queries = {
    'staging_events_copy': staging_events_copy,
    'staging_songs_copy': staging_songs_copy
    }

insert_table_queries = {
    'user_table_insert': user_table_insert,
    'song_table_insert': song_table_insert,
    'artist_table_insert': artist_table_insert,
    'time_table_insert': time_table_insert,
    'songplay_table_insert': songplay_table_insert
    }