import logging
from pyspark.sql import SparkSession
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
class TableCompareConfig:
    """
    Dataclass for table compare configurations.

    Attributes:
        limit (int): The number of rows to sample from each table.
        main_table_name (str): The name of the main Delta table.
        cloned_table_name (str): The name of the cloned Delta table.
        key_column_name (List[str]): The list of key column names used for ordering and comparison.
    """
    limit: int
    main_table_name: str
    cloned_table_name: str
    key_column_name: List[str]


class TempTableCreator:
    """
    A class to create temporary tables with 100 matched rows from the main and cloned tables.
    """

    def __init__(self, config: TableCompareConfig):
        """
        Initialize with the table compare configuration.

        :param config: An instance of TableCompareConfig containing the configuration.
        """
        self.spark = SparkSession.builder.appName("TempTableCreator").getOrCreate()
        self.config = config
        logger.info(f"Initialized with config: {self.config}")

    def create_initial_temp_tables(self):
        """
        Create initial temporary tables with the postfix '_temp_Stg'.
        """
        # Load main table and create a temporary table
        df_main = self.spark.read.format("delta").table(self.config.main_table_name)
        main_temp_stg_table = f"{self.config.main_table_name}_temp_Stg"
        df_main.write.mode("overwrite").saveAsTable(main_temp_stg_table)

        # Select distinct keys from the main temporary table
        key_columns = self.config.key_column_name
        key_columns_str = ", ".join([f"trim({col}) as {col}" for col in key_columns])
        key_table_name = f"key_{self.config.main_table_name}"
        df_keys = self.spark.sql(f"""
            SELECT DISTINCT {key_columns_str}
            FROM {main_temp_stg_table}
            LIMIT {self.config.limit}
        """)
        df_keys.write.mode("overwrite").saveAsTable(key_table_name)

        # Load cloned table
        df_cloned = self.spark.read.format("delta").table(self.config.cloned_table_name)
        cloned_temp_stg_table = f"{self.config.cloned_table_name}_temp_Stg"
        
        # Generate the join condition
        join_condition = " AND ".join([f"trim(main.{col}) = trim(keys.{col})" for col in key_columns])

        # Perform the join and select all columns from the cloned table
        df_cloned_filtered = df_cloned.alias("main").join(
            self.spark.table(key_table_name).alias("keys"), join_condition, "inner"
        ).select("main.*")
        
        # Create a temporary table for the filtered cloned table
        df_cloned_filtered.write.mode("overwrite").saveAsTable(cloned_temp_stg_table)

        logger.info(f"Initial temporary tables created: {main_temp_stg_table}, {key_table_name}, {cloned_temp_stg_table}")

    def create_final_temp_tables(self):
        """
        Create final temporary tables with the postfix '_temp' from the initial temporary tables.
        """
        main_temp_stg_table = f"{self.config.main_table_name}_temp_Stg"
        cloned_temp_stg_table = f"{self.config.cloned_table_name}_temp_Stg"
        main_temp_table = f"{self.config.main_table_name}_temp"
        cloned_temp_table = f"{self.config.cloned_table_name}_temp"

        # Create final temporary table for the main table
        self.spark.sql(f"""
            CREATE TABLE {main_temp_table} AS
            SELECT * FROM {main_temp_stg_table}
            LIMIT {self.config.limit}
        """)

        # Create final temporary table for the cloned table
        self.spark.sql(f"""
            CREATE TABLE {cloned_temp_table} AS
            SELECT * FROM {cloned_temp_stg_table}
            LIMIT {self.config.limit}
        """)

        logger.info(f"Final temporary tables created: {main_temp_table}, {cloned_temp_table}")

    def drop_initial_temp_tables(self):
        """
        Drop the initial temporary tables with the postfix '_temp_Stg'.
        """
        main_temp_stg_table = f"{self.config.main_table_name}_temp_Stg"
        cloned_temp_stg_table = f"{self.config.cloned_table_name}_temp_Stg"
        key_table_name = f"key_{self.config.main_table_name}"

        self.spark.sql(f"DROP TABLE IF EXISTS {main_temp_stg_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {key_table_name}")
        self.spark.sql(f"DROP TABLE IF EXISTS {cloned_temp_stg_table}")

        logger.info(f"Initial temporary tables dropped: {main_temp_stg_table}, {key_table_name}, {cloned_temp_stg_table}")

    def show_temp_tables(self):
        """
        Show the first few rows of the final temporary tables for verification.
        """
        main_temp_table = f"{self.config.main_table_name}_temp"
        cloned_temp_table = f"{self.config.cloned_table_name}_temp"

        logger.info(f"First few rows of {main_temp_table}:")
        self.spark.sql(f"SELECT * FROM {main_temp_table} LIMIT 10").show()

        logger.info(f"First few rows of {cloned_temp_table}:")
        self.spark.sql(f"SELECT * FROM {cloned_temp_table} LIMIT 10").show()


if __name__ == "__main__":
    # Example configuration
    config = TableCompareConfig(
        limit=100,
        main_table_name="en_poc_dev.sparta_work.user_data",
        cloned_table_name="en_poc_dev.sparta_work.user_data_stage",
        key_column_name=["clm_id"]
    )

    temp_table_creator = TempTableCreator(config)
    temp_table_creator.create_initial_temp_tables()
    temp_table_creator.create_final_temp_tables()
    temp_table_creator.drop_initial_temp_tables()
    temp_table_creator.show_temp_tables()
