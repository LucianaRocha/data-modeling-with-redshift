import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplay;"
users_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS song;"
artist_table_drop = "DROP table IF EXISTS artist;"
time_table_drop = "DROP table IF EXISTS times;"

# CREATE TABLES

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
        "userId int)"
    
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
        "year int not null)"
)

songplay_table_create = (
    "CREATE table IF NOT EXISTS songplays (" \
        "songplay_id int," \
        "start_time TIMESTAMP," \
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
        "FOREIGN KEY (artist_id) REFERENCES artists (artist_id))"
    )

users_table_create = (
    "CREATE table IF NOT EXISTS users (" \
        "user_id int," \
        "first_name varchar," \
        "last_name varchar," \
        "gender varchar," \
        "level varchar," \
        "PRIMARY KEY (user_id))"
    )

song_table_create = (
    "CREATE table IF NOT EXISTS songs (" \
        "song_id varchar NOT NULL," \
        "title varchar," \
        "artist_id varchar NOT NULL," \
        "year int," \
        "duration numeric," \
        "PRIMARY KEY (song_id))"
    )

artist_table_create = (
    "CREATE table IF NOT EXISTS artists (" \
        "artist_id varchar NOT NULL," \
        "name varchar," \
        "location varchar," \
        "latitude numeric," \
        "longitude numeric," \
        "PRIMARY KEY (artist_id))"
    )

time_table_create = (
    "CREATE table IF NOT EXISTS times (" \
        "start_time TIMESTAMP," \
        "hour int," \
        "day int," \
        "week int," \
        "month int," \
        "year int," \
        "weekday int," \
        "PRIMARY KEY (start_time))"
    )

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    format as json {}
""").format(
    config.get("S3", "LOG_DATA"),
    ARN,
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    json 'auto'
""").format(config.get("S3", "SONG_DATA"), ARN)

# FINAL TABLES

user_table_insert = (
    "INSERT INTO users(" \
        "user_id," \
        "first_name," \
        "last_name," \
        "gender," \
        "level) " \
        "VALUES (%s, %s, %s, %s, %s)" \
        "ON CONFLICT (user_id)" \
        "DO UPDATE " \
            "SET first_name  = EXCLUDED.first_name," \
                "last_name  = EXCLUDED.last_name," \
                "gender  = EXCLUDED.gender," \
                "level  = EXCLUDED.level;"
)

song_table_insert = (
    "INSERT INTO songs(" \
        "song_id," \
        "title," \
        "artist_id," \
        "year," \
        "duration)" \
        "VALUES (%s, %s, %s, %s, %s)" \
        "ON CONFLICT (song_id)" \
        "DO UPDATE " \
            "SET title  = EXCLUDED.title," \
                "artist_id  = EXCLUDED.artist_id," \
                "year  = EXCLUDED.year," \
                "duration  = EXCLUDED.duration;"
)

artist_table_insert = (
        "INSERT INTO artists(" \
        "artist_id," \
        "name," \
        "location," \
        "latitude," \
        "longitude)" \
        "VALUES (%s, %s, %s, %s, %s)" \
        "ON CONFLICT (artist_id)" \
        "DO UPDATE " \
            "SET name  = EXCLUDED.name," \
                "location = EXCLUDED.location," \
                "latitude  = EXCLUDED.latitude," \
                "longitude = EXCLUDED.longitude;"
)

time_table_insert = (
        "INSERT INTO times (" \
        "start_time," \
        "hour," \
        "day," \
        "week," \
        "month," \
        "year," \
        "weekday)" \
        "VALUES (%s, %s, %s, %s, %s, %s, %s)" \
        "ON CONFLICT (start_time)" \
        "DO UPDATE " \
            "SET hour  = EXCLUDED.hour," \
                "day = EXCLUDED.day," \
                "week  = EXCLUDED.week," \
                "month = EXCLUDED.month;"
)

songplay_table_insert = (
    "INSERT INTO songplays(" \
        "start_time," \
        "user_id," \
        "level," \
        "song_id," \
        "artist_id," \
        "session_id," \
        "location," \
        "user_agent) " \
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
) 

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create,  
    users_table_create, 
    song_table_create, 
    artist_table_create, 
    time_table_create,
    songplay_table_create
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
copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy
    ]
insert_table_queries = [
    user_table_insert, 
    song_table_insert, 
    artist_table_insert, 
    time_table_insert,
    songplay_table_insert
    ]
