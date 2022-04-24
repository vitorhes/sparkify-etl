import psycopg2
from filefinder import get_file_values, extract_datetime, log_to_df
from sql_queries import *
import pandas as pd


def etl_process(cur):
    '''
    Insert values to the 5 tables created.
    It will fetch data from the jsons located in the 'data' folder
    '''

    print("Inserting values to 'songs' table")
    try:
        values = get_file_values('./data/song_data/',['song_id', 'title', 'artist_id', 'year', 'duration'])
        for value in values:

            cur.execute("INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s,%s,%s,%s,%s)", \
                (value[0],value[1],value[2],value[3],value[4]))
        
    except psycopg2.Error as e:
        print(e)

    
    print("Inserting values to 'artists' table")
    try:
        values = get_file_values('./data/song_data/',['artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude'])
        for value in values:

            cur.execute("INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING", \
                (value[0],value[1],value[2],value[3],value[4]))
  
    except psycopg2.Error as e:
        print(e)


    print("Inserting values to 'users' table")
    try:
        values = get_file_values('./data/log_data/',['userId', 'firstName', 'lastName', 'gender', 'level'])
        for value in values:

            cur.execute("INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO NOTHING", \
                (value[0],value[1],value[2],value[3],value[4]))

    except psycopg2.Error as e:
        print(e)


    print("Inserting values to 'time' table")
    try:
        values = extract_datetime('./data/log_data/')
        for value in values:
            for i, row in values[0].iterrows():
                cur.execute("INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (start_time) DO NOTHING", \
                (row[0],row[1],row[2],row[3],row[4],row[5],row[6]))
          
    except psycopg2.Error as e:
        print(e)
        

    print("Inserting values to 'songplays' table")
    try:
        dfs = log_to_df('./data/log_data/')
        for df in dfs:
            for index, row in df.iterrows():

                cur.execute(song_select,(row.song, row.artist, row.length))
                results = cur.fetchone()

                if results:
                    songid, artistid = results
                else:
                    songid, artistid = None, None

                songplay_data = (pd.to_datetime(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
                cur.execute(songplay_table_insert, songplay_data)

       

    except psycopg2.Error as e:
        print(e)

    print("Done inserting values to all tables")
