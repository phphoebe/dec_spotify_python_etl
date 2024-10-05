from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:
    """
    A client for interacting with a PostgreSQL database using SQLAlchemy.

    Provides methods to perform common database operations such as selecting data, 
    creating tables, inserting, overwriting, and upserting records.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432,
    ):
        """
        Initialize the PostgreSqlClient with connection parameters.

        Args:
            server_name (str): The hostname of the PostgreSQL server.
            database_name (str): The name of the PostgreSQL database.
            username (str): The username for connecting to the database.
            password (str): The password for connecting to the database.
            port (int): The port number for connecting to the PostgreSQL server (default is 5432).
        """
        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def select_all(self, table: Table) -> list[dict]:
        """
        Retrieve all rows from the specified table.

        Args:
            table (Table): The SQLAlchemy Table object representing the target table.

        Returns:
            list[dict]: A list of dictionaries, each representing a row in the table.
        """
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def create_tables(self, table_schemas: dict) -> None:
        """
        Create tables based on the provided schema definitions, if they do not already exist.

        Args:
            table_schemas (dict): A dictionary where keys are table names and values are SQLAlchemy Table objects.
        """
        metadata = MetaData()
        for table_name, table in table_schemas.items():
            # Create table only if it doesn't exist
            table.create(self.engine, checkfirst=True)

    def drop_table(self, table_name: str) -> None:
        """
        Drop the specified table if it exists in the PostgreSQL database.

        Args:
            table_name (str): The name of the table to drop.
        """
        self.engine.execute(f"drop table if exists {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Insert data into the specified table.

        Args:
            data (list[dict]): A list of dictionaries representing rows to be inserted.
            table (Table): The SQLAlchemy Table object representing the target table.
            metadata (MetaData): SQLAlchemy MetaData object for managing schema.
        """
        metadata.create_all(self.engine)  # Ensure that the table is created
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Overwrite the table by dropping the existing one and re-creating it, then inserting new data.

        Args:
            data (list[dict]): A list of dictionaries representing rows to be inserted.
            table (Table): The SQLAlchemy Table object representing the target table.
            metadata (MetaData): SQLAlchemy MetaData object for managing schema.
        """
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Perform an "upsert" operation, inserting data while updating records if conflicts exist.

        Args:
            data (list[dict]): A list of dictionaries representing rows to be upserted.
            table (Table): The SQLAlchemy Table object representing the target table.
            metadata (MetaData): SQLAlchemy MetaData object for managing schema.
        """
        metadata.create_all(self.engine)  # Ensure that the table is created
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns},
        )
        self.engine.execute(upsert_statement)
