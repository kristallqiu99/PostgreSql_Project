import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files

def song_data_processor(cur, filepath):

    for song in get_files(filepath):
        # Read song file
        df = pd.read_json(song, lines=True)

        # Select relevant fields
        song_data = list(
            df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
        )

        artist_data = list(
            df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
        )

        # Insert into tables
        cur.execute(song_table_insert, song_data)
        cur.execute(artist_table_insert, artist_data)

def convert_ts(ts):
    # Convert ts to datetime
    ts_dt = pd.to_datetime(ts, unit='ms')

    # Extract and save time data
    time_data = [
        ts_dt.values, ts_dt.dt.hour.values, ts_dt.dt.day.values,
        ts_dt.dt.isocalendar().week.values, ts_dt.dt.month.values, ts_dt.dt.year.values,
        ts_dt.dt.weekday.values
    ]

    return time_data

def get_songplay_data(cur, row):
    # Query song_id and artist_id from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    result = cur.fetchone()

    # Extract song_id and artist_id from query
    if result is None:
        song_id, artist_id = None, None
    else:
        song_id, artist_id = result

    # Generate songplay data
    songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, song_id, artist_id, row.sessionId,
                     row.location, row.userAgent]

    return songplay_data

def log_data_processor(cur, filepath):

    for log in get_files(filepath):
        # Read log file
        df = pd.read_json(log, lines=True)

        # Filter data
        df_filtered = df.loc[df.page == 'NextSong']

        # Convert time
        time_data = convert_ts(df_filtered['ts'])
        time_cols = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
        time_df = pd.DataFrame(dict(zip(time_cols, time_data)))

        # Select user data
        user_df = df_filtered[['userId', 'firstName', 'lastName', 'gender', 'level']]

        # Insert into tables
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, row)

        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)

        # Songplay data
        for i, row in df_filtered.iterrows():
            cur.execute(songplay_table_insert, get_songplay_data(cur, row))

if __name__ == '__main__':
    # Connect to DB
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # Insert data
    song_data_processor(cur, './data/song_data/')
    log_data_processor(cur, './data/log_data/')
    conn.commit()

    conn.close()
    print('All done!')