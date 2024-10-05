import logging
import time


class PipelineLogging:
    """
    A class to handle logging for the ETL pipeline execution. 
    It logs information to both a file and the console.

    Attributes:
        pipeline_name (str): The name of the pipeline being logged.
        log_folder_path (str): The directory where the log file will be saved.
        logger (logging.Logger): The logger instance used to log messages.
        file_path (str): Path of the log file.
    """

    def __init__(self, pipeline_name: str, log_folder_path: str):
        """
        Initializes the PipelineLogging class, sets up log handlers to log both 
        to a file and to the console.

        Args:
            pipeline_name (str): Name of the pipeline being executed.
            log_folder_path (str): Path to the folder where the log file will be saved.
        """
        self.pipeline_name = pipeline_name
        self.log_folder_path = log_folder_path

        # Create a logger instance for the pipeline
        logger = logging.getLogger(pipeline_name)
        logger.setLevel(logging.INFO)

        # Generate the log file path using the pipeline name and current timestamp
        self.file_path = f"{self.log_folder_path}/{self.pipeline_name}_{time.time()}.log"

        # File handler logs to a file
        file_handler = logging.FileHandler(self.file_path)
        file_handler.setLevel(logging.INFO)

        # Stream handler logs to the console (stdout)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        # Define a log format for consistency in the log messages
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Set the formatter for both the file and stream handlers
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # Attach handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        # Assign the logger to the class attribute for use in the pipeline
        self.logger = logger

    def get_logs(self) -> str:
        """
        Retrieve the log content from the log file.

        Returns:
            str: A string containing the content of the log file.
        """
        with open(self.file_path, "r") as file:
            return "".join(file.readlines())
