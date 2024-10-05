import os

import requests
import pandas as pd
from dotenv import load_dotenv
import base64

from sqlalchemy import (
    create_engine,
    Table,
    Column,
    MetaData,
    Integer,
    String
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL


def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Function to retrieve access token from Spotify API using client credentials.

    Parameters:
        client_id (str): The Spotify Client ID.
        client_secret (str): The Spotify Client Secret.

    Returns:
        str: Spotify API access token.
    """
    auth_str = f"{client_id}:{client_secret}"
    auth_bytes = auth_str.encode('utf-8')
    auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')

    url = "https://accounts.spotify.com/api/token"
    headers = {
        'Authorization': f"Basic {auth_base64}",
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'client_credentials'
    }

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")


def extract_playlist_data(access_token: str, playlist_id: str) -> tuple[dict, pd.DataFrame]:
    """
    Function to extract playlist data and track items from Spotify API.

    Parameters:
        access_token (str): Spotify API access token.
        playlist_id (str): The Spotify playlist ID.

    Returns:
        tuple: 
            dict: Full playlist data in JSON format.
            pd.DataFrame: Normalized DataFrame for track items from the playlist.
    """
    playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    headers = {
        'Authorization': f"Bearer {access_token}"
    }

    response = requests.get(playlist_url, headers=headers)
    if response.json() is not None:
        playlist_data = response.json()
    else:
        raise Exception(
            f"Error fetching playlist data: {response.status_code}")

    # Normalize the track items into a DataFrame
    df_tracks_items = pd.json_normalize(playlist_data['tracks']['items'])

    return playlist_data, df_tracks_items


def extract_artist_data(access_token: str, df_tracks_items: pd.DataFrame) -> list[dict]:
    """
    Function to extract artist data from Spotify API based on artist IDs found in tracks.

    Parameters:
        access_token (str): Spotify API access token.
        df_tracks_items (pd.DataFrame): DataFrame containing track data with nested artist information.

    Returns:
        list[dict]: List of artist data dictionaries from Spotify API.
    """
    # Extract artist IDs from the nested 'track.artists' field in the track data
    # Each track can have multiple artists, so the lambda function creates a comma-separated string of artist IDs
    df_tracks_items['artist_ids'] = df_tracks_items['track.artists'].apply(
        lambda x: ', '.join([artist['id'] for artist in x]
                            ) if isinstance(x, list) else None
    )

    # Split the comma-separated artist IDs into individual IDs and use 'explode' to flatten them into a single column
    # 'unique()' is used to ensure that only distinct artist IDs are retained, avoiding redundant API calls
    artist_ids = df_tracks_items['artist_ids'].str.split(
        ', ').explode().unique()

    artist_data = []
    base_url = "https://api.spotify.com/v1/artists/"
    headers = {
        'Authorization': f"Bearer {access_token}"
    }

    # Iterate through each unique artist ID and make API calls to fetch detailed artist information
    for artist_id in artist_ids:
        response = requests.get(base_url + artist_id, headers=headers)
        if response.status_code == 200:
            artist_data.append(response.json())
        else:
            raise Exception(
                f"Error fetching artist data: {response.status_code}")

    return artist_data


def transform(df_tracks_items: pd.DataFrame, playlist_data: dict, artist_data: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Function to transform playlist and artist data into DataFrames for tracks, albums, and artists.

    Parameters:
        df_tracks_items (pd.DataFrame): DataFrame of track items.
        playlist_data (dict): Full playlist data including playlist-level fields.
        artist_data (list[dict]): List of artist data dictionaries.

    Returns:
        tuple: 
            pd.DataFrame: Tracks data.
            pd.DataFrame: Albums data.
            pd.DataFrame: Artists data.
    """
    # Step 1: Process tracks data from playlist
    df_tracks = df_tracks_items[[
        'track.id', 'track.name', 'track.popularity', 'track.duration_ms',
        'track.album.id', 'artist_ids', 'added_at'
    ]]

    # Rename columns for consistency
    df_tracks = df_tracks.rename(columns={
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
    ]].drop_duplicates()

    # Rename columns for albums
    df_albums = df_albums.rename(columns={
        'track.album.id': 'album_id',
        'track.album.name': 'album_name',
        'track.album.release_date': 'album_release_date',
        'track.album.total_tracks': 'album_total_tracks'
    })

    # Step 3: Process artists data (from the artist_data API response)
    df_artists = pd.json_normalize(artist_data)
    df_artists = df_artists[['id', 'name', 'genres', 'popularity']].rename(columns={
        'id': 'artist_id',
        'name': 'artist_name',
        'genres': 'artist_genres',
        'popularity': 'artist_popularity'
    })

    # Return cleaned DataFrames for tracks, albums, and artists
    return df_tracks, df_albums, df_artists


def load(
    df_tracks: pd.DataFrame,
    df_albums: pd.DataFrame,
    df_artists: pd.DataFrame,
    db_user: str,
    db_password: str,
    db_server_name: str,
    db_database_name: str,
):
    """
    Load DataFrames for tracks, albums, and artists into a PostgreSQL database using upsert logic.

    Parameters:
        df_tracks (pd.DataFrame): DataFrame containing tracks data.
        df_albums (pd.DataFrame): DataFrame containing albums data.
        df_artists (pd.DataFrame): DataFrame containing artists data.
        db_user (str): Database username.
        db_password (str): Database password.
        db_server_name (str): Database server name (host).
        db_database_name (str): Database name.

    """
    # Create connection to the database
    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=db_user,
        password=db_password,
        host=db_server_name,
        port=5432,
        database=db_database_name,
    )

    engine = create_engine(connection_url)

    meta = MetaData()

    # Define schema details for each table (tracks, albums, artists)
    tables = {
        "tracks": {
            "df": df_tracks,
            "columns": [
                Column("track_id", String, primary_key=True),
                Column("track_name", String),
                Column("track_popularity", Integer),
                Column("track_duration_ms", Integer),
                Column("album_id", String),
                Column("artist_id", String),
                Column("playlist_id", String),
                Column("playlist_name", String),
                Column("snapshot_id", String),
                Column("added_at", String),
            ],
            "pk": "track_id",
        },
        "albums": {
            "df": df_albums,
            "columns": [
                Column("album_id", String, primary_key=True),
                Column("album_name", String),
                Column("album_release_date", String),
                Column("album_total_tracks", Integer),
            ],
            "pk": "album_id",
        },
        "artists": {
            "df": df_artists,
            "columns": [
                Column("artist_id", String, primary_key=True),
                Column("artist_name", String),
                Column("artist_genres", String),
                Column("artist_popularity", Integer),
            ],
            "pk": "artist_id",
        },
    }

    # Create tables dynamically and perform upserts
    for table_name, table_info in tables.items():
        # Define the table schema dynamically
        table = Table(table_name, meta, *table_info["columns"])
        meta.create_all(engine)  # Create the table if it does not exist

        # Prepare the insert statement
        insert_statement = postgresql.insert(table).values(
            table_info["df"].to_dict(orient="records")
        )

        # Prepare the upsert statement (on_conflict_do_update)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=[table_info["pk"]],
            set_={
                c.key: c
                for c in insert_statement.excluded
                if c.key != table_info["pk"]
            },
        )

        # Execute the upsert statement
        engine.execute(upsert_statement)


if __name__ == "__main__":
    load_dotenv()

    # Spotify API credentials
    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

    # Database credentials
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")

    # Step 1: Get access token
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET)

    # Step 2: Extract playlist data and track items
    playlist_id = '31FWVQBp3WQydWLNhO0ACi'  # Lofi Girl's favorite playlist
    playlist_data, df_tracks_items = extract_playlist_data(
        access_token, playlist_id)

    # Step 3: Extract artist data using the unique artist IDs
    artist_data = extract_artist_data(access_token, df_tracks_items)

    # Step 4: Transform normalized track items, playlist, and artist data into DataFrames
    df_tracks, df_albums, df_artists = transform(
        df_tracks_items, playlist_data, artist_data)

    # Step 5: Load the data into PostgreSQL
    load(
        df_tracks=df_tracks,
        df_albums=df_albums,
        df_artists=df_artists,
        db_user=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_server_name=SERVER_NAME,
        db_database_name=DATABASE_NAME
    )

    print("ETL process completed successfully.")
