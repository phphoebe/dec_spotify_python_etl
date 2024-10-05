import pytest
from sqlalchemy import Table, Column, Integer, String, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from dotenv import load_dotenv
import os


@pytest.fixture
def setup_postgresql_client():
    """
    Set up the PostgreSQL client for testing.
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
def setup_table_schema():
    """
    Set up a simple table schema for testing.
    """
    metadata = MetaData()
    test_table = Table(
        "test_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
    )
    return test_table, metadata


def test_create_and_insert(setup_postgresql_client, setup_table_schema):
    """
    Test creating a table and inserting data.
    """
    postgresql_client = setup_postgresql_client
    test_table, metadata = setup_table_schema

    # Drop table if it already exists (clean start)
    postgresql_client.drop_table("test_table")

    # Create table
    postgresql_client.create_tables({"test_table": test_table})

    # Insert data into the table
    data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
    postgresql_client.write_to_table(
        data=data, table_name="test_table", table_schemas={"test_table": test_table})

    # Verify that data was inserted
    result = postgresql_client.select_all(test_table)
    assert len(result) == 2
    assert result[0]["name"] == "test1"
    assert result[1]["name"] == "test2"
