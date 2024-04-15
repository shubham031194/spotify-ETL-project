import json
import spotipy
import os
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    CLIENT_ID = os.environ.get('CLIENT_ID')
    CLIENT_KEY = os.environ.get('CLIENT_KEY')
    URL = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    
    client_cred_manager = SpotifyClientCredentials(client_id=CLIENT_ID,client_secret=CLIENT_KEY)
    sp = spotipy.Spotify(client_credentials_manager=client_cred_manager)
    track_id = URL.split('/')[-1]
    raw_data = sp.playlist_tracks(track_id)
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    client = boto3.client('s3')
    client.put_object(
        Bucket="spotify-etl-project-shubham1996",
        Key="raw_data/to_process/" + filename,
        Body=json.dumps(raw_data)
        )