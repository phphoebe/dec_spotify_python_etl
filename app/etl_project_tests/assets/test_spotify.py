import os
import pytest
import pandas as pd
from etl_project.assets.spotify import transform, load_data
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, Column, String, Integer, MetaData
from dotenv import load_dotenv


@pytest.fixture
def setup_postgresql_client():
    """
    Fixture to set up a PostgreSQL client for testing.

    This fixture loads environment variables to establish a connection 
    with the PostgreSQL database. It returns a `PostgreSqlClient` object 
    that can be used to interact with the database during tests.

    Returns:
        PostgreSqlClient: A client instance connected to the PostgreSQL database.
    """
    load_dotenv()
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    return postgresql_client


@pytest.fixture
def setup_table_metadata():
    """
    Fixture to define table schemas for the `tracks`, `albums`, and `artists` tables.

    This fixture returns a dictionary of SQLAlchemy `Table` objects representing
    the schema of each table used during testing. These schemas include column names,
    data types, and primary keys for each table.

    Returns:
        dict: A dictionary with table names as keys and SQLAlchemy Table objects as values.
    """
    metadata = MetaData()
    tracks_table = Table(
        "tracks",
        metadata,
        Column("track_id", String, primary_key=True),
        Column("track_name", String),
        Column("track_popularity", Integer),
        Column("track_duration_ms", Integer),
        Column("track_added_at", String),
        Column("album_id", String),
        Column("artist_id", String),  # artist_id is used in the schema
    )
    albums_table = Table(
        "albums",
        metadata,
        Column("album_id", String, primary_key=True),
        Column("album_name", String),
        Column("album_release_date", String),
        Column("album_total_tracks", Integer),
    )
    artists_table = Table(
        "artists",
        metadata,
        Column("artist_id", String, primary_key=True),
        Column("artist_name", String),
        Column("artist_genres", String),
        Column("artist_popularity", Integer),
    )
    return {
        "tracks": tracks_table,
        "albums": albums_table,
        "artists": artists_table
    }


@pytest.fixture
def setup_transformed_data():
    """
    Fixture to provide mock transformed data for testing.

    This fixture provides sample playlist metadata, tracks, and artist data 
    to simulate the structure and content of actual Spotify playlist data.

    Returns:
        dict: A dictionary containing playlist metadata, a DataFrame for tracks, and a list for artist data.
    """
    return {
        "playlist_metadata": {
            "id": "playlist123",
            "name": "Test Playlist",
            "snapshot_id": "snapshot123"
        },
        "df_tracks_items": pd.DataFrame(
            [
                {
                    "track.id": "track1",
                    "track.name": "Track 1",
                    "track.popularity": 85,
                    "track.duration_ms": 180000,
                    "track.album.id": "album1",
                    "track.album.name": "Album 1",
                    "track.album.release_date": "2023-01-01",
                    "track.album.total_tracks": 10,
                    "track.artists": [{"id": "artist1"}],  # Nested artist info
                    "added_at": "2023-10-01T12:00:00Z"
                },
                {
                    "track.id": "track2",
                    "track.name": "Track 2",
                    "track.popularity": 90,
                    "track.duration_ms": 240000,
                    "track.album.id": "album2",
                    "track.album.name": "Album 2",
                    "track.album.release_date": "2023-02-01",
                    "track.album.total_tracks": 12,
                    "track.artists": [{"id": "artist2"}],  # Nested artist info
                    "added_at": "2023-10-02T12:00:00Z"
                }
            ]
        ),
        "artist_data": [
            {
                "id": "artist1",
                "name": "Artist 1",
                "genres": ["pop", "rock"],
                "popularity": 80
            },
            {
                "id": "artist2",
                "name": "Artist 2",
                "genres": ["jazz"],
                "popularity": 70
            }
        ]
    }


def test_transform(setup_transformed_data):
    """
    Test the transformation logic, ensuring the correct structure for tracks, albums, and artists.

    This test validates that the `transform` function correctly processes the input DataFrame 
    and artist data, and produces DataFrames for tracks, albums, and artists with the 
    expected columns and data.

    Args:
        setup_transformed_data (dict): Mock data containing playlist metadata, tracks, and artist data.

    Asserts:
        - Track, album, and artist DataFrames contain the correct columns and data.
    """
    playlist_metadata = setup_transformed_data["playlist_metadata"]
    df_tracks_items = setup_transformed_data["df_tracks_items"]
    artist_data = setup_transformed_data["artist_data"]

    # Ensure artist_ids is created from nested artist data
    df_tracks_items['artist_ids'] = df_tracks_items['track.artists'].apply(
        lambda x: ', '.join([artist['id'] for artist in x]
                            ) if isinstance(x, list) else None
    )

    # Perform the transformation
    df_tracks, df_albums, df_artists = transform(
        df_tracks_items, playlist_metadata, artist_data)

    # Test Tracks DataFrame
    assert "track_id" in df_tracks.columns
    assert "track_name" in df_tracks.columns
    assert "artist_id" in df_tracks.columns

    # Test Albums DataFrame
    assert "album_id" in df_albums.columns
    assert "album_name" in df_albums.columns
    assert "album_release_date" in df_albums.columns
    assert "album_total_tracks" in df_albums.columns

    # Test Artists DataFrame
    assert "artist_id" in df_artists.columns
    assert "artist_name" in df_artists.columns
    assert "artist_genres" in df_artists.columns
    assert "artist_popularity" in df_artists.columns


def test_load_data(setup_postgresql_client, setup_transformed_data, setup_table_metadata):
    """
    Test loading transformed data into PostgreSQL.

    This test validates the functionality of the `load_data` function by:
    1. Creating the required tables in PostgreSQL.
    2. Loading the transformed tracks DataFrame into the database.
    3. Verifying that the data is successfully inserted by fetching it back.

    Args:
        setup_postgresql_client (PostgreSqlClient): PostgreSQL client fixture.
        setup_transformed_data (dict): Mock data containing playlist metadata, tracks, and artist data.
        setup_table_metadata (dict): Table schemas for PostgreSQL.

    Asserts:
        - The data is successfully inserted into the database and matches the input data.
    """
    postgresql_client = setup_postgresql_client
    data_dict = {
        "tracks": setup_transformed_data["df_tracks_items"]
    }
    table_schemas = setup_table_metadata

    # Ensure the table exists before loading data
    postgresql_client.create_tables(table_schemas)

    # Adjust DataFrame for tracks to align with the tracks schema
    df_tracks_items = data_dict["tracks"].rename(columns={
        'track.id': 'track_id',
        'track.name': 'track_name',
        'track.album.id': 'album_id',
        'track.artists': 'artist_ids',  # artist_ids column instead of artist_id
        'track.popularity': 'track_popularity',
        'track.duration_ms': 'track_duration_ms',
        'added_at': 'track_added_at'
    })

    # Rename artist_ids to artist_id to match table schema
    df_tracks_items['artist_id'] = df_tracks_items['artist_ids']

    # Update the DataFrame back into the data_dict
    data_dict["tracks"] = df_tracks_items[[
        'track_id', 'track_name', 'track_popularity', 'track_duration_ms', 'album_id', 'artist_id', 'track_added_at'
    ]]

    # Test loading the data into the PostgreSQL database
    load_data(
        data_dict=data_dict,
        postgresql_client=postgresql_client,
        table_schemas=table_schemas,
        load_method="upsert"
    )

    # Verify data has been inserted by fetching the data back
    fetched_tracks = postgresql_client.select_all(table_schemas["tracks"])
    assert len(fetched_tracks) == len(df_tracks_items)

    # Clean up by dropping the table
    postgresql_client.engine.execute("drop table if exists tracks cascade")
