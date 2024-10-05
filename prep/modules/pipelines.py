import os
from dotenv import load_dotenv
from assets import extract_playlist_data, extract_artist_data, transform, load_data

if __name__ == "__main__":
    load_dotenv()

    # Spotify API credentials from .env file
    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

    # Database credentials from .env file
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")

    # Playlist ID for extraction
    playlist_id = '31FWVQBp3WQydWLNhO0ACi'  # Example playlist ID

    # Step 1: Extract playlist data and track items
    print("Extracting playlist data...")
    playlist_data, df_tracks_items = extract_playlist_data(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        playlist_id=playlist_id
    )

    # Step 2: Extract artist data based on track information
    print("Extracting artist data...")
    artist_data = extract_artist_data(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        df_tracks_items=df_tracks_items
    )

    # Step 3: Transform the extracted data into separate DataFrames for tracks, albums, and artists
    print("Transforming data...")
    df_tracks, df_albums, df_artists = transform(
        df_tracks_items=df_tracks_items,
        playlist_data=playlist_data,
        artist_data=artist_data
    )

    # Step 4: Load the transformed data into PostgreSQL
    print("Loading data into PostgreSQL...")
    load_data(
        df_tracks=df_tracks,
        df_albums=df_albums,
        df_artists=df_artists,
        db_user=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_server_name=SERVER_NAME,
        db_database_name=DATABASE_NAME
    )

    print("ETL process completed successfully.")
