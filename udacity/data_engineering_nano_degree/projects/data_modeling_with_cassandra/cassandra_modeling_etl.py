# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

def execute_music_session_length_query(session):
    try:
        query = """
          SELECT artist, song, length FROM udacity.music_session_length
          WHERE itemInSession = 4 AND sessionId = 338
        """

        rows = session.execute(query)

        for row in rows:
            print(row.artist + " " + row.song + " " + str(row.length))
    except Exception  as e:
        print(e)

def execute_music_session_artist_query(session):
    try:
        query = """
          SELECT artist, song, firstName, lastName FROM udacity.music_session_artist
          WHERE userId = 10 AND sessionId = 182
        """

        rows = session.execute(query)

        for row in rows:
            print(row.artist + " " + row.song + " " + row.firstname + " " + row.lastname)
    except Exception as e:
        print(e)

def get_music_session_length_query():
    ## TO-DO: Assign the INSERT statements into the `query` variable
    query = """
        INSERT INTO music_session_length(artist,
                                         firstName,
                                         gender,
                                         itemInSession,
                                         lastName,
                                         length,
                                         level,
                                         location,
                                         sessionId,
                                         song,
                                         userId)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    return query

def get_music_session_artist_query():
    ## TO-DO: Assign the INSERT statements into the `query` variable
    query = """
        INSERT INTO music_session_artist(artist,
                                         firstName,
                                         gender,
                                         itemInSession,
                                         lastName,
                                         length,
                                         level,
                                         location,
                                         sessionId,
                                         song,
                                         userId)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    return query

def verify_data_load(session, table):
    query = "SELECT COUNT(*) FROM udacity." + table
    row = session.execute(query)[0]

    if row.count > 0:
        print("Data has been loaded successfully to " + table)
    else:
        print("Data load error has occurred " + table)
        exit(1)


def shutdown(cluster, session):
    session.shutdown()
    cluster.shutdown()

def load_data(session, query):
    # We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
    file = 'event_datafile_new.csv'

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
            ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
            artist = line[0]
            first_name = line[1]
            gender = line[2]
            item_in_session = int(line[3])
            last_name = line[4]
            length = float(line[5])
            level = line[6]
            location = line[7]
            session_id = int(line[8])
            song = line[9]
            user_id = int(line[10])
            session.execute(query, (artist,
                                    first_name,
                                    gender,
                                    item_in_session,
                                    last_name,
                                    length,
                                    level,
                                    location,
                                    session_id,
                                    song,
                                    user_id))

def setup_database():
    drop_table_music_session_length = """
      DROP TABLE IF EXISTS udacity.music_session_length
    """
    drop_table_music_session_artist = """
      DROP TABLE IF EXISTS udacity.music_session_artist
    """
    drop_table_music_session_user = """
      DROP TABLE IF EXISTS udacity.music_session_user
    """

    create_table_music_session_length = """
       CREATE TABLE udacity.music_session_length(artist text,
                                                 firstName text,
                                                 gender text,
                                                 itemInSession int,
                                                 lastName text,
                                                 length float,
                                                 level text,
                                                 location text,
                                                 sessionId int,
                                                 song text,
                                                 userId int,
                                                 PRIMARY KEY(sessionId, itemInSession))
    """
    create_table_music_session_artist = """
        CREATE TABLE udacity.music_session_artist(artist text,
                                                  firstName text,
                                                  gender text,
                                                  itemInSession int,
                                                  lastName text,
                                                  length float,
                                                  level text,
                                                  location text,
                                                  sessionId int,
                                                  song text,
                                                  userId int,
                                                  PRIMARY KEY((userId, sessionId), itemInSession))
                                        """
    create_table_music_session_user = """

                                      """

    try:
        # This should make a connection to a Cassandra instance your local machine
        # (127.0.0.1)
        cluster = Cluster(['127.0.0.1'])

        # To establish connection and begin executing queries, need a session
        session = cluster.connect()
        session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS udacity
                        WITH REPLICATION =
                        {'class': 'SimpleStrategy', 'replication_factor':1}
                        """
                        )
        session.set_keyspace('udacity')
        session.execute(drop_table_music_session_length)
        session.execute(drop_table_music_session_artist)
        session.execute(drop_table_music_session_user)
        session.execute(create_table_music_session_length)
        session.execute(create_table_music_session_artist)
        return (cluster, session)
    except Exception as e:
        print(e)


def stage_data():
    # checking your current working directory
    print(os.getcwd())

    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):

    # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        print(file_path_list)

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:

    # reading csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

     # extracting each data row one by one and append it
            for line in csvreader:
                print(line)
                full_data_rows_list.append(line)

    # uncomment the code below if you would like to get total number of rows
    print(len(full_data_rows_list))
    # uncomment the code below if you would like to check to see what the list of event data rows will look like
    print(full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    # check the number of rows in your csv file
    with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
        print(sum(1 for line in f))

stage_data()
cluster, session = setup_database()


# Session Length Data Query
# music_session_length_query = get_music_session_length_query()
# load_data(session, music_session_length_query)
# verify_data_load(session, "music_session_length")
# execute_music_session_length_query(session)

# Session Aritist Data Query
music_session_arist_query = get_music_session_artist_query()
load_data(session, music_session_arist_query)
verify_data_load(session, "music_session_artist")
execute_music_session_artist_query(session)

shutdown(cluster, session)

