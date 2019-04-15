import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json

def open_file(filepath, multi_line):
    with open(filepath, 'r') as file:
        if multi_line:
            data = list()
            for lines in file:
                data.append(json.loads(lines))

            df = pd.DataFrame(data)
        else:
            data = json.load(file)
            df = pd.DataFrame(data, index=[0])

    return df


def process_song_file(cur, filepath):
    # open song file
    df = open_file(filepath, False)

    # insert song record
    song_data = df['song_id'][0], df['title'][0], df['artist_id'][0], int(df['year'][0]), float(df['duration'][0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df['artist_id'][0], df['artist_name'][0], df['artist_location'][0], df['artist_latitude'][0], df['artist_longitude'][0]
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    # open log file
    df = open_file(filepath, True)

    is_next_song = df['page'] == 'NextSong'
    df_is_next_song_df = df[is_next_song]
    df_is_next_song_df['ts_datetime'] = pd.to_datetime(df_is_next_song_df['ts'],unit='ms')

    for i, row in df_is_next_song_df.iterrows():
        time_data = row['ts'], row['ts_datetime'].hour, row['ts_datetime'].day, row['ts_datetime'].week, row['ts_datetime'].year, row['ts_datetime'].weekday()
        cur.execute(time_table_insert, time_data)

        user_data = row['userId'], row['firstName'], row['lastName'], row['gender'], row['level']
        cur.execute(user_table_insert, user_data)

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = songid, row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent']
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()