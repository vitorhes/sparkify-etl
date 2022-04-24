# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
        (
            songplay_id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            user_id INTEGER,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INTEGER, 
            location VARCHAR,
            user_agent VARCHAR
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
        (
            user_id INTEGER PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            gender CHAR(1),
            level VARCHAR
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
        (
            song_id VARCHAR PRIMARY KEY,
            title VARCHAR NOT NULL,
            artist_id VARCHAR NOT NULL,
            year INTEGER CHECK (year >= 0),
            duration DECIMAL NOT NULL
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
        (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            location VARCHAR,
            latitude DECIMAL,
            longitude DECIMAL
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
        (
            start_time VARCHAR PRIMARY KEY,
            hour INTEGER NOT NULL CHECK (hour >= 0),
            day INTEGER NOT NULL CHECK (day >= 0),
            week INTEGER NOT NULL CHECK (week >= 0),
            month INTEGER NOT NULL CHECK (month >= 0),
            year INTEGER NOT NULL CHECK (year >= 0),
            weekday VARCHAR NOT NULL
        );
""")


song_select = ("""
    SELECT
        songs.song_id AS song_id,
        songs.artist_id AS artist_id
    FROM
        songs JOIN artists
        ON songs.artist_id=artists.artist_id
    WHERE
        songs.title = %s AND 
        artists.name = %s AND 
        songs.duration = %s  
        
""")

#insert queries

songplay_table_insert = ("""
    INSERT INTO songplays
        (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) 
    DO NOTHING;
""")

drop_table_list = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,time_table_drop]

table_list = [song_table_create,user_table_create,artist_table_create,time_table_create,songplay_table_create]

tables_names_list = ["songs","users","artists","time","songplays"]
