from dotenv import load_dotenv
import os
from etl_project.assets.spotify import extract_playlist_data, extract_artist_data, transform, load_data
from etl_project.connectors.spotify import SpotifyAccessTokenClient, SpotifyAPIClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, Column, Integer, String, MetaData, inspect
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
import yaml
from pathlib import Path
import schedule
import time


def pipeline(config: dict, pipeline_logging: PipelineLogging):
    """
    The main ETL pipeline for extracting playlist and artist data from the Spotify API,
    transforming it, and loading it into a PostgreSQL database.

    Args:
        config (dict): Pipeline configuration data, including API credentials and playlist ID.
        pipeline_logging (PipelineLogging): Logger for tracking pipeline execution steps.
    """
    pipeline_logging.logger.info("Starting pipeline run")

    # Set up environment variables
    pipeline_logging.logger.info("Getting pipeline environment variables")
    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    # Initialize Spotify API clients
    pipeline_logging.logger.info("Creating Spotify Access Token Client")
    spotify_access_token_client = SpotifyAccessTokenClient(
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )

    pipeline_logging.logger.info("Creating Spotify API Client")
    spotify_api_client = SpotifyAPIClient(
        access_token_client=spotify_access_token_client
    )

    # Extract playlist metadata and track items
    pipeline_logging.logger.info("Extracting playlist data from Spotify API")
    playlist_id = config.get("playlist_id")
    playlist_metadata, df_tracks_items = extract_playlist_data(
        spotify_api_client=spotify_api_client, playlist_id=playlist_id
    )

    # Extract artist data based on track items
    pipeline_logging.logger.info("Extracting artist data from Spotify API")
    artist_data = extract_artist_data(
        spotify_api_client=spotify_api_client, df_tracks_items=df_tracks_items
    )

    # Transform the extracted data
    pipeline_logging.logger.info("Transforming dataframes")
    df_tracks, df_albums, df_artists = transform(
        df_tracks_items, playlist_metadata, artist_data
    )

    # Load data into PostgreSQL database
    pipeline_logging.logger.info("Loading data to PostgreSQL")
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    # Define metadata object and table schemas
    metadata = MetaData()

    # Define the database table structures
    table_schemas = {
        "tracks": Table(
            "tracks",
            metadata,
            Column("track_id", String, primary_key=True),
            Column("track_name", String),
            Column("track_popularity", Integer),
            Column("track_duration_ms", Integer),
            Column("track_added_at", String),
            Column("album_id", String),
            Column("artist_id", String),
            Column("playlist_id", String),
            Column("playlist_name", String),
            Column("snapshot_id", String),
        ),
        "albums": Table(
            "albums",
            metadata,
            Column("album_id", String, primary_key=True),
            Column("album_name", String),
            Column("album_release_date", String),
            Column("album_total_tracks", Integer),
        ),
        "artists": Table(
            "artists",
            metadata,
            Column("artist_id", String, primary_key=True),
            Column("artist_name", String),
            Column("artist_genres", String),
            Column("artist_popularity", Integer),
        ),
    }

    # Create the necessary database tables if they do not already exist
    postgresql_client.create_tables(table_schemas=table_schemas)

    # Prepare data dictionary for loading into the database
    data_dict = {
        "tracks": df_tracks,
        "albums": df_albums,
        "artists": df_artists
    }

    # Load data into PostgreSQL using the specified method (e.g., upsert)
    load_data(
        data_dict=data_dict,
        postgresql_client=postgresql_client,
        table_schemas=table_schemas,
        load_method="upsert",  # Can be 'insert', 'upsert', or 'overwrite'
    )

    # Create database views from SQL files if they don't already exist
    pipeline_logging.logger.info("Inspecting database views")
    engine = postgresql_client.engine  # Get engine from the PostgreSqlClient
    inspector = inspect(engine)

    # Get the SQL folder path from the pipeline configuration
    sql_folder_path = config.get("sql_folder_path")

    for sql_file in os.listdir(sql_folder_path):
        # Extract view name from the SQL file name
        view_name = sql_file.split(".")[0]
        if view_name not in inspector.get_view_names():
            pipeline_logging.logger.info(
                f"View {view_name} does not exist - Creating view")
            with open(os.path.join(sql_folder_path, sql_file), 'r') as f:
                sql_query = f.read()
                engine.execute(f"create view {view_name} as {sql_query};")
                pipeline_logging.logger.info(
                    f"Successfully created view {view_name}")
        else:
            pipeline_logging.logger.info(
                f"View {view_name} already exists in the database")

    # Log the successful completion of the pipeline
    pipeline_logging.logger.info("Pipeline run successful")


def run_pipeline(
    pipeline_name: str,
    postgresql_logging_client: PostgreSqlClient,
    pipeline_config: dict,
):
    """
    Run the ETL pipeline and log its progress, handling success and failure.

    Args:
        pipeline_name (str): The name of the pipeline.
        postgresql_logging_client (PostgreSqlClient): Client for logging the pipeline run status.
        pipeline_config (dict): Configuration settings for the pipeline.
    """
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_config.get("name"),
        log_folder_path=pipeline_config.get("config").get("log_folder_path"),
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    try:
        metadata_logger.log()  # Log the start of the pipeline
        pipeline(config=pipeline_config.get("config"),
                 pipeline_logging=pipeline_logging)
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # Log the successful completion of the pipeline
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(
            f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # Log the failure of the pipeline
        pipeline_logging.logger.handlers.clear()


if __name__ == "__main__":
    load_dotenv()

    # Get environment variables for logging database connection
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    # Load pipeline configuration from YAML file
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a 'name' key for the pipeline name.")

    # Initialize PostgreSQL client for logging
    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    # Schedule the pipeline to run at specified intervals
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline,
        pipeline_name=PIPELINE_NAME,
        postgresql_logging_client=postgresql_logging_client,
        pipeline_config=pipeline_config,
    )

    # Continuously run scheduled tasks
    while True:
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
