"""
This script creates temporary tables with 100 rows from the main table and the cloned table.

It ensures the manifest files are the same and refreshes the cloned table if necessary.

Classes:
    - TableCompareOptions: Dataclass for table compare options.
    - TableCompareConfig: Dataclass for table compare configurations.
    - TempTableCreator: A class to create temporary tables with 100 rows from the main and cloned tables.

Functions:
    - __init__: Initialize with the table compare configuration.
    - get_manifest_file: Get the manifest file path of the Delta table.
    - refresh_clone_if_needed: Compare manifest files of the main and cloned tables and refresh the clone if needed.
    - create_temp_tables: Create temporary tables with 100 rows from both the main and cloned tables.
    - show_temp_tables: Show the first few rows of the temporary tables for verification.
"""

import logging
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from dataclasses import dataclass
from typing import List

# Set up a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

@dataclass
class TableCompareOptions:
    """
    Dataclass for table compare options.
    """
    limit: int
    main_table_name: str
    cloned_table_name: str
    key_column_name: List[str]

@dataclass
class TableCompareConfig:
    """
    Dataclass for table compare configurations.
    """
    limit: int
    main_table_name: str
    cloned_table_name: str
    key_column_name: List[str]


class TempTableCreator:
    """
    A class to create temporary tables with 100 rows from the main and cloned tables.
    """

    def __init__(self, config: TableCompareConfig):
        """
        Initialize with the table compare configuration.

        :param config: An instance of TableCompareConfig containing the configuration.
        """
        self.spark = SparkSession.builder.appName("TempTableCreator").getOrCreate()
        self.config = config

    def get_manifest_file(self, table):
        """
        Get the manifest file path of the Delta table.

        :param table: The Delta table name.
        :return: The manifest file path.
        """
        delta_table = DeltaTable.forName(self.spark, table)
        return delta_table.history().select("operationMetrics").collect()[0].asDict()[
            "operationMetrics"
        ]["write.path"]

    def refresh_clone_if_needed(self):
        """
        Compare manifest files of the main and cloned tables and refresh the clone if needed.
        """
        main_manifest = self.get_manifest_file(self.config.main_table_name)
        cloned_manifest = self.get_manifest_file(self.config.cloned_table_name)

        if main_manifest != cloned_manifest:
            logger.info("Manifest files differ. Refreshing the cloned table...")
            self.spark.sql(f"REFRESH TABLE {self.config.cloned_table_name}")
            logger.info("Cloned table refreshed.")

    def create_temp_tables(self):
        """
        Create temporary tables with 100 rows from both the main and cloned tables.
        """
        self.refresh_clone_if_needed()

        # Load main table
        df_main = self.spark.read.format("delta").table(self.config.main_table_name)
        # Load cloned table
        df_cloned = self.spark.read.format("delta").table(self.config.cloned_table_name)

        # Select 100 random rows from each table
        df_main_sample = df_main.orderBy(self.config.key_column_name).limit(self.config.limit)
        df_cloned_sample = df_cloned.orderBy(self.config.key_column_name).limit(self.config.limit)

        # Create temporary views
        df_main_sample.createOrReplaceTempView("main_temp_table")
        df_cloned_sample.createOrReplaceTempView("cloned_temp_table")

        logger.info("Temporary tables created: main_temp_table, cloned_temp_table")

    def show_temp_tables(self):
        """
        Show the first few rows of the temporary tables for verification.
        """
        self.spark.sql("SELECT * FROM main_temp_table").show()
        self.spark.sql("SELECT * FROM cloned_temp_table").show()


if __name__ == "__main__":
    # Example configuration
    config = TableCompareConfig(
        limit=100,
        main_table_name="main_table_name",
        cloned_table_name="cloned_table_name",
        key_column_name=["key_column1", "key_column2"]
    )

    temp_table_creator = TempTableCreator(config)
    temp_table_creator.create_temp_tables()
    temp_table_creator.show_temp_tables()
