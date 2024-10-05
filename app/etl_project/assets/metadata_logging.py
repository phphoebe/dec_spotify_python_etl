from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime
from sqlalchemy import Table, Column, Integer, String, MetaData, JSON
from sqlalchemy import insert, select, func


class MetaDataLoggingStatus:
    """Data class for log status"""

    RUN_START = "start"
    RUN_SUCCESS = "success"
    RUN_FAILURE = "fail"


class MetaDataLogging:
    def __init__(
        self,
        pipeline_name: str,
        postgresql_client: PostgreSqlClient,
        config: dict = {},
        log_table_name: str = "pipeline_logs",
    ):
        self.pipeline_name = pipeline_name
        self.log_table_name = log_table_name
        self.postgresql_client = postgresql_client
        self.config = config
        self.metadata = MetaData()

        # Define the table schema for logging
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
        self.run_id: int = self._get_run_id()

    def _create_log_table(self) -> None:
        """Create log table if it does not exist."""
        # Use the updated `create_tables` function from PostgreSqlClient
        self.postgresql_client.create_tables(table_schemas=self.table_schema)

    def _get_run_id(self) -> int:
        """Gets the next run id. Sets run id to 1 if no run id exists."""
        self._create_log_table()  # Ensure the log table is created

        # Retrieve the max run_id for the current pipeline
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
        """Writes pipeline metadata log to a database"""
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Prepare the insert statement
        insert_statement = insert(self.table_schema[self.log_table_name]).values(
            pipeline_name=self.pipeline_name,
            timestamp=timestamp,
            run_id=self.run_id,
            status=status,
            config=self.config,
            logs=logs,
        )
        # Execute the insert statement
        self.postgresql_client.engine.execute(insert_statement)
