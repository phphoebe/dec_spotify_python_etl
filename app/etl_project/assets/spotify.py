import pandas as pd
from etl_project.connectors.spotify import SpotifyAPIClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import MetaData


def extract_playlist_data(spotify_api_client: SpotifyAPIClient, playlist_id: str) -> tuple[dict, pd.DataFrame]:
    """
    Extract playlist metadata and track items from the Spotify API, handling pagination.

    Args:
        spotify_api_client (SpotifyAPIClient): The client to interact with the Spotify API.
        playlist_id (str): The ID of the Spotify playlist to extract data from.

    Returns:
        tuple: 
            dict: Playlist metadata, including details like playlist name and snapshot ID.
            pd.DataFrame: A DataFrame containing normalized track items from the playlist.
    """
    # Fetch playlist metadata and full track list (handling pagination)
    playlist_metadata, tracks_list = spotify_api_client.get_playlist_data(
        playlist_id)

    # Normalize the track items into a DataFrame
    df_tracks_items = pd.json_normalize(tracks_list)

    return playlist_metadata, df_tracks_items


def extract_artist_data(spotify_api_client: SpotifyAPIClient, df_tracks_items: pd.DataFrame) -> list[dict]:
    """
    Extract artist data from the Spotify API based on artist IDs present in the tracks.

    Args:
        spotify_api_client (SpotifyAPIClient): The client to interact with the Spotify API.
        df_tracks_items (pd.DataFrame): A DataFrame containing track items with nested artist information.

    Returns:
        list[dict]: A list of artist data dictionaries retrieved from the Spotify API.
    """
    # Extract artist IDs from track data
    df_tracks_items['artist_ids'] = df_tracks_items['track.artists'].apply(
        lambda x: ', '.join([artist['id'] for artist in x]
                            ) if isinstance(x, list) else None
    )

    # Split and flatten the artist IDs into unique values
    artist_ids = df_tracks_items['artist_ids'].str.split(
        ', ').explode().unique()

    # Fetch artist data from the API
    artist_data = [spotify_api_client.get_artist(
        artist_id) for artist_id in artist_ids]

    return artist_data


def transform(
    df_tracks_items: pd.DataFrame,
    playlist_metadata: dict,
    artist_data: list[dict]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Transform raw playlist and artist data into structured DataFrames for tracks, albums, and artists.

    Args:
        df_tracks_items (pd.DataFrame): DataFrame containing raw track data.
        playlist_metadata (dict): Playlist metadata containing information like playlist name and snapshot ID.
        artist_data (list[dict]): List of dictionaries containing artist data from the Spotify API.

    Returns:
        tuple: 
            pd.DataFrame: Tracks DataFrame with processed track details.
            pd.DataFrame: Albums DataFrame with processed album details.
            pd.DataFrame: Artists DataFrame with processed artist details.
    """
    # Step 1: Process track data from playlist
    df_tracks = df_tracks_items[[
        'track.id', 'track.name', 'track.popularity', 'track.duration_ms',
        'track.album.id', 'artist_ids', 'added_at'
    ]].rename(columns={
        'track.id': 'track_id',
        'track.name': 'track_name',
        'track.popularity': 'track_popularity',
        'track.duration_ms': 'track_duration_ms',
        'added_at': 'track_added_at',
        'track.album.id': 'album_id',
        'artist_ids': 'artist_id'
    })

    # Add playlist-level metadata fields to the tracks DataFrame
    df_tracks['playlist_id'] = playlist_metadata['id']
    df_tracks['playlist_name'] = playlist_metadata['name']
    df_tracks['snapshot_id'] = playlist_metadata['snapshot_id']

    # Step 2: Process album data, removing duplicates
    df_albums = df_tracks_items[[
        'track.album.id', 'track.album.name', 'track.album.release_date', 'track.album.total_tracks'
    ]].drop_duplicates().rename(columns={
        'track.album.id': 'album_id',
        'track.album.name': 'album_name',
        'track.album.release_date': 'album_release_date',
        'track.album.total_tracks': 'album_total_tracks'
    })

    # Step 3: Process artist data from the API response
    df_artists = pd.json_normalize(artist_data)[['id', 'name', 'genres', 'popularity']].rename(columns={
        'id': 'artist_id',
        'name': 'artist_name',
        'genres': 'artist_genres',
        'popularity': 'artist_popularity'
    })

    return df_tracks, df_albums, df_artists


def load_data(
    data_dict: dict[str, pd.DataFrame],
    postgresql_client: PostgreSqlClient,
    table_schemas: dict,
    load_method: str = "overwrite",
) -> None:
    """
    Load transformed DataFrames into PostgreSQL using the specified loading method.

    Args:
        data_dict (dict): A dictionary where the keys are table names and the values are DataFrames.
        postgresql_client (PostgreSqlClient): PostgreSQL client instance for interacting with the database.
        table_schemas (dict): A dictionary of SQLAlchemy Table objects representing the schema.
        load_method (str): The method for loading data ('insert', 'upsert', or 'overwrite').
    """
    # Map the load method to the appropriate PostgreSQL client function
    method_mapping = {
        "insert": postgresql_client.insert,
        "upsert": postgresql_client.upsert,
        "overwrite": postgresql_client.overwrite
    }

    if load_method not in method_mapping:
        raise Exception(
            "Invalid load method. Please specify one of: [insert, upsert, overwrite]")

    # Loop through the tables and apply the specified method for each DataFrame
    for table_name, df in data_dict.items():
        if table_name in table_schemas:
            table = table_schemas[table_name]
            method_mapping[load_method](
                data=df.to_dict(orient="records"),
                table=table,
                metadata=MetaData()  # Create new metadata for each operation
            )
        else:
            raise Exception(f"Table schema for {table_name} not found.")
