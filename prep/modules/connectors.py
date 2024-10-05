import requests
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    MetaData,
    Integer,
    String
)
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql
import pandas as pd
import base64

# Step 1: Spotify API connector to fetch access token


def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Fetch Spotify access token using client credentials.

    Args:
        client_id (str): Spotify Client ID.
        client_secret (str): Spotify Client Secret.

    Returns:
        str: Access token for API calls.
    """
    auth_str = f"{client_id}:{client_secret}"
    auth_bytes = auth_str.encode('utf-8')
    auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')

    url = "https://accounts.spotify.com/api/token"
    headers = {
        'Authorization': f"Basic {auth_base64}",
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {'grant_type': 'client_credentials'}

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(
            f"Failed to get access token: {response.status_code} - {response.text}")


# Step 2: Create database engine using SQLAlchemy
def get_database_engine(
    db_user: str, db_password: str, db_server_name: str, db_database_name: str
) -> Engine:
    """
    Create and return a SQLAlchemy engine for the database.

    Args:
        db_user (str): Database username.
        db_password (str): Database password.
        db_server_name (str): Database server name.
        db_database_name (str): Database name.

    Returns:
        Engine: SQLAlchemy engine for the PostgreSQL database.
    """
    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=db_user,
        password=db_password,
        host=db_server_name,
        port=5432,
        database=db_database_name,
    )
    engine = create_engine(connection_url)
    return engine


# Step 3: Function to dynamically create tables and perform inserts/upserts
def write_to_database(df: pd.DataFrame, table_name: str, engine: Engine):
    """
    Create table dynamically and perform inserts/upserts.

    Args:
        engine (Engine): SQLAlchemy engine for the PostgreSQL database.
        df (pd.DataFrame): DataFrame containing data to insert.
        table_name (str): Name of the table in the database.
        metadata (MetaData): SQLAlchemy MetaData object for table creation.
    """
    meta = MetaData()

    # Define schema details for each table
    table_schemas = {
        "tracks": [
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
        "albums": [
            Column("album_id", String, primary_key=True),
            Column("album_name", String),
            Column("album_release_date", String),
            Column("album_total_tracks", Integer),
        ],
        "artists": [
            Column("artist_id", String, primary_key=True),
            Column("artist_name", String),
            Column("artist_genres", String),
            Column("artist_popularity", Integer),
        ],
    }

    # Dynamically create the table based on its schema
    table = Table(table_name, meta, *table_schemas[table_name])
    meta.create_all(engine)  # Create the table if it doesn't exist

    # Prepare the insert statement
    insert_statement = postgresql.insert(
        table).values(df.to_dict(orient="records"))

    # Prepare the upsert statement (on_conflict_do_update)
    # Assume the first column is the primary key
    pk_column = table_schemas[table_name][0].name
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=[pk_column],
        set_={c.key: c for c in insert_statement.excluded if c.key != pk_column},
    )

    # Execute the upsert statement
    engine.execute(upsert_statement)
