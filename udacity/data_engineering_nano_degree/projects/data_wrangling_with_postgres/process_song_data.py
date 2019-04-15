import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json


def process_song_file(cur, filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)
    # open song file
    df = pd.DataFrame(data, index=[0])

    # insert song record
    song_data = df['song_id'][0], df['title'][0], df['artist_id'][0], int(df['year'][0]), float(df['duration'][0])
    # cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df['artist_id'][0], df['artist_name'][0], df['artist_location'][0], df['artist_latitude'][0], df['artist_longitude'][0]
    cur.execute(artist_table_insert, artist_data)

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
    # process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()