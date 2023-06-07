Step 1 -To solve this we have to undergo creating the pipeline using ETL (Extract Tranform and Load)

intalling the python libraries inorder to perform the process

pip install pandas boto3 '''(boto3 boto3 is the Amazon Web Services (AWS) SDK for Python. such as S3 (Simple Storage Service).'''
import pandas as pd
import boto3 

'''now here we need to difne the bucket where the JSON-formatted data is stored let say filename is 'songsdata' '''
''' we also need the log data a JSON file containing log entries related to user activity, such as song plays, user information, and timestamps.'''

bucket_name = 'afzalsongsdata' (this is the bucket name were the data is stored)
song_data_file = 'songsdata.json'
log_data_file = 'logsdata.json'

--performing extract and Tranform operation for songsdata file
def extract_transform_song_data():
    # Read the JSON file into a Pandas DataFrame
    song_df = pd.read_json(songsdata, lines=True)

    # Perform any necessary data transformations
    # Example: Select relevant columns and rename them
    transformed_song_df = song_df[['song_id', 'title', 'artist', 'duration']].copy()
    transformed_song_df.rename(columns={'song_id': 'song_id', 'title': 'song_title', 'artist': 'artist_name', 'duration': 'song_duration'}, inplace=True)

    return transformed_song_df

--performing extract and Tranform operation for logsdata file

def extract_transform_log_data():
    # Read the JSON file into a Pandas DataFrame
    log_df = pd.read_json(logsata, lines=True)

    # Perform any necessary data transformations
    # Example: Filter rows by a specific condition
    transformed_log_df = log_df[log_df['page'] == 'NextSong'].copy()

    return transformed_log_df
    
#finally we need to call the functions

transformed_song_data = extract_transform_song_data()
transformed_log_data = extract_transform_log_data()

