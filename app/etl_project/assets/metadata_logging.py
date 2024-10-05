from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime
from sqlalchemy import Table, Column, Integer, String, MetaData, JSON
from sqlalchemy import insert, select, func


class MetaDataLoggingStatus:
    """
    Constants representing the status of the pipeline run.
    """
    RUN_START = "start"
    RUN_SUCCESS = "success"
    RUN_FAILURE = "fail"


class MetaDataLogging:
    """
    A class responsible for logging pipeline metadata (e.g., run status, config, logs)
    to a PostgreSQL database. It ensures that each run of the pipeline is logged,
    including information about when the run started, whether it succeeded, and any logs or configuration used.
    """

    def __init__(
        self,
        pipeline_name: str,
        postgresql_client: PostgreSqlClient,
        config: dict = {},
        log_table_name: str = "pipeline_logs",
    ):
        """
        Initializes the MetaDataLogging class, which creates or updates log entries in the PostgreSQL database.

        Args:
            pipeline_name (str): The name of the pipeline to log.
            postgresql_client (PostgreSqlClient): An instance of PostgreSqlClient to interact with the database.
            config (dict, optional): Configuration settings to be logged. Defaults to an empty dictionary.
            log_table_name (str, optional): Name of the log table in the database. Defaults to 'pipeline_logs'.
        """
        self.pipeline_name = pipeline_name
        self.log_table_name = log_table_name
        self.postgresql_client = postgresql_client
        self.config = config
        self.metadata = MetaData()

        # Define the schema for the log table
        self.table_schema = {
            self.log_table_name: Table(
                self.log_table_name,
                self.metadata,
                Column("pipeline_name", String, primary_key=True),
                Column("run_id", Integer, primary_key=True),
                Column("timestamp", String, primary_key=True),
                Column("status", String, primary_key=True),
                Column("config", JSON),
                Column("logs", String),
            )
        }

        # Retrieve the next run ID to log this run
        self.run_id: int = self._get_run_id()

    def _create_log_table(self) -> None:
        """
        Creates the log table in the PostgreSQL database if it does not exist.
        """
        self.postgresql_client.create_tables(table_schemas=self.table_schema)

    def _get_run_id(self) -> int:
        """
        Gets the next available run ID for the current pipeline. 
        If no previous runs exist, sets the run ID to 1.

        Returns:
            int: The next run ID.
        """
        self._create_log_table()  # Ensure the log table is created

        # Get the maximum run_id for the current pipeline
        result = self.postgresql_client.engine.execute(
            select(func.max(self.table_schema[self.log_table_name].c.run_id))
            .where(self.table_schema[self.log_table_name].c.pipeline_name == self.pipeline_name)
        ).first()

        run_id = result[0] if result else None
        return 1 if run_id is None else run_id + 1

    def log(
        self,
        status: MetaDataLoggingStatus = MetaDataLoggingStatus.RUN_START,
        timestamp: datetime = None,
        logs: str = None,
    ) -> None:
        """
        Writes a log entry to the database, including the pipeline name, run ID, timestamp, status, configuration, and logs.

        Args:
            status (MetaDataLoggingStatus): The current status of the pipeline run (e.g., start, success, or fail).
            timestamp (datetime, optional): The timestamp of the log. Defaults to the current time.
            logs (str, optional): Logs related to the pipeline run. Defaults to None.
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Insert log entry into the table
        insert_statement = insert(self.table_schema[self.log_table_name]).values(
            pipeline_name=self.pipeline_name,
            timestamp=timestamp,
            run_id=self.run_id,
            status=status,
            config=self.config,
            logs=logs,
        )
        # Execute the insert statement in the database
        self.postgresql_client.engine.execute(insert_statement)
