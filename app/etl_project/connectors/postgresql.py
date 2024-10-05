from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:
    """
    A client for querying PostgreSQL database.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

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
        Select all data from the specified table in the PostgreSQL database.

        Args:
            table (Table): The SQLAlchemy Table object representing the target table.

        Returns:
            list[dict]: A list of dictionaries representing the rows in the table.
        """
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def create_tables(self, table_schemas: dict) -> None:
        """
        Create tables based on the provided table schemas.

        Args:
            table_schemas (dict): A dictionary containing table schemas.
        """
        metadata = MetaData()
        for table_name, table in table_schemas.items():
            table.create(self.engine, checkfirst=True)

    def drop_table(self, table_name: str) -> None:
        """
        Drop the specified table from the PostgreSQL database.

        Args:
            table_name (str): The name of the table to drop.
        """
        self.engine.execute("drop table if exists {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Insert data into the specified table.

        Args:
            data (list[dict]): List of dictionaries containing the data to insert.
            table (Table): SQLAlchemy Table object representing the target table.
            metadata (MetaData): SQLAlchemy MetaData object for managing schema.
        """
        metadata.create_all(self.engine)
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Overwrite data in the specified table by dropping the table first and re-creating it.

        Args:
            data (list[dict]): List of dictionaries containing the data to insert.
            table (Table): SQLAlchemy Table object representing the target table.
            metadata (MetaData): SQLAlchemy MetaData object for managing schema.
        """
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        """
        Upsert data into the specified table, updating records if conflicts exist.

        Args:
            data (list[dict]): List of dictionaries containing the data to upsert.
            table (Table): SQLAlchemy Table object representing the target table.
            metadata (MetaData): SQLAlchemy MetaData object for managing schema.
        """
        metadata.create_all(self.engine)
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        self.engine.execute(upsert_statement)
