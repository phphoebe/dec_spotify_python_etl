import pandas as pd
from connectors import get_access_token, write_to_database, get_database_engine
import requests


def extract_playlist_data(client_id: str, client_secret: str, playlist_id: str) -> tuple[dict, pd.DataFrame]:
    """
    Extract playlist data and track items from Spotify API.

    Args:
        client_id (str): Spotify Client ID.
        client_secret (str): Spotify Client Secret.
        playlist_id (str): Spotify playlist ID.

    Returns:
        tuple: 
            dict: Full playlist data.
            pd.DataFrame: Normalized DataFrame for track items.
    """
    # Step 1: Get access token using the client ID and secret
    access_token = get_access_token(client_id, client_secret)

    # Step 2: Fetch playlist data
    playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    headers = {'Authorization': f"Bearer {access_token}"}

    response = requests.get(playlist_url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            f"Error fetching playlist data: {response.status_code}")

    playlist_data = response.json()
    df_tracks_items = pd.json_normalize(playlist_data['tracks']['items'])
    return playlist_data, df_tracks_items


def extract_artist_data(client_id: str, client_secret: str, df_tracks_items: pd.DataFrame) -> list[dict]:
    """
    Extract artist data from Spotify API based on artist IDs found in tracks.

    Args:
        client_id (str): Spotify Client ID.
        client_secret (str): Spotify Client Secret.
        df_tracks_items (pd.DataFrame): DataFrame containing track data with nested artist information.

    Returns:
        list[dict]: List of artist data dictionaries from Spotify API.
    """
    # Step 1: Get access token using the client ID and secret
    access_token = get_access_token(client_id, client_secret)

    # Step 2: Extract artist IDs from track data
    df_tracks_items['artist_ids'] = df_tracks_items['track.artists'].apply(
        lambda x: ', '.join([artist['id'] for artist in x]
                            ) if isinstance(x, list) else None
    )

    # Split and flatten the artist IDs
    artist_ids = df_tracks_items['artist_ids'].str.split(
        ', ').explode().unique()

    artist_data = []
    base_url = "https://api.spotify.com/v1/artists/"
    headers = {'Authorization': f"Bearer {access_token}"}

    # Fetch artist data for each unique artist ID
    for artist_id in artist_ids:
        response = requests.get(base_url + artist_id, headers=headers)
        if response.status_code == 200:
            artist_data.append(response.json())
        else:
            raise Exception(
                f"Error fetching artist data: {response.status_code}")

    return artist_data


def transform(
    df_tracks_items: pd.DataFrame,
    playlist_data: dict,
    artist_data: list[dict]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Transform playlist and artist data into DataFrames for tracks, albums, and artists.

    Args:
        df_tracks_items (pd.DataFrame): DataFrame of track items.
        playlist_data (dict): Full playlist data.
        artist_data (list[dict]): List of artist data dictionaries.

    Returns:
        tuple: DataFrames for tracks, albums, and artists.
    """
    # Step 1: Process tracks data from playlist
    df_tracks = df_tracks_items[[
        'track.id', 'track.name', 'track.popularity', 'track.duration_ms',
        'track.album.id', 'artist_ids', 'added_at'
    ]].rename(columns={
        'track.id': 'track_id',
        'track.name': 'track_name',
        'track.popularity': 'track_popularity',
        'track.duration_ms': 'track_duration_ms',
        'track.album.id': 'album_id',
        'added_at': 'added_at',
        'artist_ids': 'artist_id'  # Rename artist_ids for clarity
    })

    # Add playlist-level fields to tracks
    df_tracks['playlist_id'] = playlist_data['id']
    df_tracks['playlist_name'] = playlist_data['name']
    df_tracks['snapshot_id'] = playlist_data['snapshot_id']

    # Step 2: Process albums data (removing duplicates)
    df_albums = df_tracks_items[[
        'track.album.id', 'track.album.name', 'track.album.release_date', 'track.album.total_tracks'
    ]].drop_duplicates().rename(columns={
        'track.album.id': 'album_id',
        'track.album.name': 'album_name',
        'track.album.release_date': 'album_release_date',
        'track.album.total_tracks': 'album_total_tracks'
    })

    # Step 3: Process artists data from the artist API response
    df_artists = pd.json_normalize(artist_data)[['id', 'name', 'genres', 'popularity']].rename(columns={
        'id': 'artist_id',
        'name': 'artist_name',
        'genres': 'artist_genres',
        'popularity': 'artist_popularity'
    })

    return df_tracks, df_albums, df_artists


def load_data(
    df_tracks: pd.DataFrame,
    df_albums: pd.DataFrame,
    df_artists: pd.DataFrame,
    db_user: str,
    db_password: str,
    db_server_name: str,
    db_database_name: str
):
    """
    Load DataFrames for tracks, albums, and artists into a PostgreSQL database using upsert logic.

    Args:
        df_tracks (pd.DataFrame): DataFrame with track data.
        df_albums (pd.DataFrame): DataFrame with album data.
        df_artists (pd.DataFrame): DataFrame with artist data.
        db_user (str): Database username.
        db_password (str): Database password.
        db_server_name (str): Database server name.
        db_database_name (str): Database name.
    """
    # Get the database engine
    engine = get_database_engine(
        db_user=db_user,
        db_password=db_password,
        db_server_name=db_server_name,
        db_database_name=db_database_name
    )

    # Define a dictionary mapping DataFrames to table names
    tables_to_load = {
        'tracks': df_tracks,
        'albums': df_albums,
        'artists': df_artists
    }

    # Loop over the dictionary to dynamically load each DataFrame into the respective table
    for table_name, df in tables_to_load.items():
        write_to_database(df=df, table_name=table_name, engine=engine)
