import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df2 = pd.read_json(filepath,lines=True)

    # insert song record
    song_data_df=df2[['song_id','title','artist_id','duration','year']]
    song_data=song_data_df.values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data_df = df2[['artist_id','artist_latitude',
                          'artist_longitude','artist_location','artist_name']]
    artist_data=artist_data_df.values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df_nextsong = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    dt = pd.to_datetime(df_nextsong['ts'],unit='ms')
    t=pd.DataFrame(dt)
    t['timeStamp']=df_nextsong['ts']
    t['startTime']=t['ts'].apply(lambda x:x.time())
    t['hour']=t['ts'].apply(lambda x:x.hour)
    t['minutes']=t['ts'].apply(lambda x:x.minute)
    t['seconds']=t['ts'].apply(lambda x:x.second)
    t['startDay']=t['ts'].apply(lambda x:x.date())
    t['startDay']=pd.to_datetime(t['startDay']).astype(str)
    t['sessionId']=df_nextsong['sessionId']
    
    # insert time data records
    time_df = t[['sessionId','timeStamp','hour','minutes','seconds','startDay']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','location']]
    user_df=user_df.drop_duplicates(subset='userId')
    user_df=user_df.reset_index()
    indexAge = user_df[(user_df['gender']!='M')&(user_df['gender']!='F') ].index
    dummy=user_df.drop(indexAge , inplace=True)
    user_df=user_df[['userId','firstName','lastName','gender','location']]
    user_df['userId']=user_df['userId'].astype(int)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

def process_songplay(cur, filepath):
    df = pd.read_json(filepath,lines=True)
    df = df[df['page']=='NextSong']
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # insert songplay record
        songplay_data = [row.userId,row.ts,songid,artistid,
                        row.level,row.itemInSession,row.page,row.userAgent,row.length]
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    process_data(cur, conn, filepath='data/log_data', func=process_songplay)

    conn.close()


if __name__ == "__main__":
    main()