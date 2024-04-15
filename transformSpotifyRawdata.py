import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(raw_data):
    album_list = []
    for row in raw_data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        total_tracks = row['track']['album']['total_tracks']
        urls = row['track']['album']['external_urls']['spotify']
        album_data = {'id': album_id,
                      'name': album_name,
                      'release_date': album_release_date,
                      'total_tracks': total_tracks,
                      'urls': urls}
        album_list.append(album_data)
    return album_list

def artists(raw_data):
    artist_list = []
    for row in raw_data['items']:
        for artist in row['track']['artists']:
            artist_datapoint = {
                                'artist_id': artist['id'],
                                'artist_name': artist['name'],
                                'artist_url': artist['href']}
            artist_list.append(artist_datapoint)
    return artist_list

def songs(raw_data):
    song_list = []
    for row in raw_data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_datapoint = {
            'song_id': song_id,
            'song_name': song_name,
            'duration_ms': song_duration,
            'url': song_url,
            'popularity': song_popularity,
            'added_at': song_added,
            'album_id': album_id,
            'artist_id': artist_id
        }
        song_list.append(song_datapoint)
    return song_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spotify-etl-project-shubham1996"
    key = "raw_data/to_process/"
    
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        album_list = album(data)
        artists_list = artists(data)
        song_list = songs(data)
        
        album_data_df = pd.DataFrame.from_dict(album_list)
        album_data_df = album_data_df.drop_duplicates(subset=['id'])
        album_data_df['release_date'] = pd.to_datetime(album_data_df['release_date'])
        
        artists_data_df = pd.DataFrame.from_dict(artists_list)
        artists_data_df = artists_data_df.drop_duplicates(subset=['artist_id'])
        
        song_data_df = pd.DataFrame.from_dict(song_list)
        song_data_df['added_at'] = pd.to_datetime(song_data_df['added_at'])
        
        Bucket = "spotify-etl-project-shubham1996"
        album_filename = 'transformed_data/album_data/album_transformed_' + str(datetime.now) + '.csv'
        album_buffer = StringIO()
        album_data_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_filename, Body=album_content)
        
        artists_filename = 'transformed_data/artists_data/artists_transformed_' + str(datetime.now) + '.csv'
        artists_buffer = StringIO()
        artists_data_df.to_csv(artists_buffer, index=False)
        artists_content = artists_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artists_filename, Body=artists_content)
        
        song_filename = 'transformed_data/songs_data/song_transformed_' + str(datetime.now) + '.csv'
        song_buffer = StringIO()
        song_data_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_filename, Body=song_content)
                        
        s3_resource = boto3.resource('s3')
        for key in spotify_keys:
            copy_source = {
                'Bucket': Bucket,
                'Key': key
            }
            s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
            s3_resource.Object(Bucket, key).delete()
                        
                        