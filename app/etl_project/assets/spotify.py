import pandas as pd
from etl_project.connectors.spotify import SpotifyAPIClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import MetaData


def extract_playlist_data(spotify_api_client: SpotifyAPIClient, playlist_id: str) -> tuple[dict, pd.DataFrame]:
    """
    Extract playlist data and track items from Spotify API.

    Args:
        spotify_api_client (SpotifyAPIClient): An instance of SpotifyAPIClient to interact with Spotify API.
        playlist_id (str): The Spotify playlist ID.

    Returns:
        tuple: 
            dict: Full playlist data.
            pd.DataFrame: Normalized DataFrame for track items.
    """
    # Fetch playlist data using the correct method name
    playlist_data = spotify_api_client.get_playlist_data(playlist_id)

    # Normalize the track items into a DataFrame
    df_tracks_items = pd.json_normalize(playlist_data['tracks']['items'])

    return playlist_data, df_tracks_items


def extract_artist_data(spotify_api_client: SpotifyAPIClient, df_tracks_items: pd.DataFrame) -> list[dict]:
    """
    Extract artist data from Spotify API based on artist IDs found in tracks.

    Args:
        spotify_api_client (SpotifyAPIClient): The Spotify API client instance.
        df_tracks_items (pd.DataFrame): DataFrame containing track data with nested artist information.

    Returns:
        list[dict]: List of artist data dictionaries from Spotify API.
    """
    # Extract artist IDs from track data
    df_tracks_items['artist_ids'] = df_tracks_items['track.artists'].apply(
        lambda x: ', '.join([artist['id'] for artist in x]
                            ) if isinstance(x, list) else None
    )

    # Split and flatten the artist IDs
    artist_ids = df_tracks_items['artist_ids'].str.split(
        ', ').explode().unique()

    # Fetch artist data using the get_artist method in SpotifyAPIClient
    artist_data = [spotify_api_client.get_artist(
        artist_id) for artist_id in artist_ids]

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
    data_dict: dict[str, pd.DataFrame],
    postgresql_client: PostgreSqlClient,
    table_schemas: dict,
    load_method: str = "overwrite",
) -> None:
    """
    Load DataFrames into PostgreSQL using the specified method (insert, upsert, overwrite).

    Args:
        data_dict (dict): Dictionary where keys are table names and values are DataFrames.
        postgresql_client (PostgreSqlClient): PostgreSQL client instance to interact with the database.
        table_schemas (dict): Table schema definitions.
        load_method (str): Load method to use. Can be 'insert', 'upsert', or 'overwrite'.
    """
    # Define the method mapping based on the load_method argument
    method_mapping = {
        "insert": postgresql_client.insert,
        "upsert": postgresql_client.upsert,
        "overwrite": postgresql_client.overwrite
    }

    if load_method not in method_mapping:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]")

    # Loop through the tables and dataframes to dynamically apply the method
    for table_name, df in data_dict.items():
        if table_name in table_schemas:
            # Get the table object from schema
            table = table_schemas[table_name]
            method_mapping[load_method](
                data=df.to_dict(orient="records"),
                table=table,  # Pass the table object directly, not table_name
                metadata=MetaData()  # Assuming the metadata is re-created as needed
            )
        else:
            raise Exception(f"Table schema for {table_name} not found.")
