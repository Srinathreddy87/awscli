"""
This script creates temporary tables with 100 rows from the main table and the cloned table.
It ensures the manifest files are the same and refreshes the cloned table if necessary.
"""

import logging
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Set up a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


class TempTableCreator:
    """
    A class to create temporary tables with 100 rows from the main and cloned tables.
    """

    def __init__(self, main_table, cloned_table, key_column):
        """
        Initialize with the main table, cloned table names, and key column.

        :param main_table: Name of the main Delta table
        :param cloned_table: Name of the cloned Delta table
        :param key_column: The key column used for selecting rows
        """
        self.spark = SparkSession.builder.appName("TempTableCreator").getOrCreate()
        self.main_table = main_table
        self.cloned_table = cloned_table
        self.key_column = key_column

    def get_manifest_file(self, table):
        """
        Get the manifest file path of the Delta table.

        :param table: The Delta table name
        :return: The manifest file path
        """
        delta_table = DeltaTable.forName(self.spark, table)
        return delta_table.history().select("operationMetrics").collect()[0].asDict()["operationMetrics"]["write.path"]

    def refresh_clone_if_needed(self):
        """
        Compare manifest files of the main and cloned tables and refresh the clone if needed.
        """
        main_manifest = self.get_manifest_file(self.main_table)
        cloned_manifest = self.get_manifest_file(self.cloned_table)

        if main_manifest != cloned_manifest:
            logger.info("Manifest files differ. Refreshing the cloned table...")
            self.spark.sql(f"REFRESH TABLE {self.cloned_table}")
            logger.info("Cloned table refreshed.")

    def create_temp_tables(self):
        """
        Create temporary tables with 100 rows from both the main and cloned tables.
        """
        self.refresh_clone_if_needed()

        # Load main table
        df_main = self.spark.read.format("delta").table(self.main_table)
        # Load cloned table
        df_cloned = self.spark.read.format("delta").table(self.cloned_table)

        # Select 100 random rows from each table
        df_main_sample = df_main.orderBy(self.key_column).limit(100)
        df_cloned_sample = df_cloned.orderBy(self.key_column).limit(100)

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
    # Replace with your actual table names and key column
    main_table_name = "main_table_name"
    cloned_table_name = "cloned_table_name"
    key_column_name = "key_column"

    temp_table_creator = TempTableCreator(main_table_name, cloned_table_name, key_column_name)
    temp_table_creator.create_temp_tables()
    temp_table_creator.show_temp_tables()
