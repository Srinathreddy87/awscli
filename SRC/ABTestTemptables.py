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
        # Load main table and create a temporary view
        df_main = self.spark.read.format("delta").table(self.config.main_table_name)
        main_temp_stg_view = f"{self.config.main_table_name}_temp_Stg"
        df_main.createOrReplaceTempView(main_temp_stg_view)

        # Select distinct keys from the main temporary view
        key_columns = self.config.key_column_name
        key_columns_str = ", ".join([f"trim({col}) as {col}" for col in key_columns])
        df_keys = self.spark.sql(f"""
            SELECT DISTINCT {key_columns_str}
            FROM {main_temp_stg_view}
            LIMIT {self.config.limit}
        """)
        df_keys.createOrReplaceTempView("keys_temp_Stg")

        # Load cloned table
        df_cloned = self.spark.read.format("delta").table(self.config.cloned_table_name)
        cloned_temp_stg_view = f"{self.config.cloned_table_name}_temp_Stg"
        
        # Generate the join condition
        join_condition = " AND ".join([f"trim(main.{col}) = trim(keys.{col})" for col in key_columns])

        # Perform the join and select all columns from the cloned table
        df_cloned_filtered = df_cloned.alias("main").join(
            self.spark.table("keys_temp_Stg").alias("keys"), join_condition, "inner"
        ).select("main.*")
        
        # Create a temporary view for the filtered cloned table
        df_cloned_filtered.createOrReplaceTempView(cloned_temp_stg_view)

        logger.info(f"Initial temporary tables created: {main_temp_stg_view}, keys_temp_Stg, {cloned_temp_stg_view}")

    def create_final_temp_tables(self):
        """
        Create final temporary tables with the postfix '_temp' from the initial temporary tables.
        """
        main_temp_stg_view = f"{self.config.main_table_name}_temp_Stg"
        cloned_temp_stg_view = f"{self.config.cloned_table_name}_temp_Stg"
        main_temp_view = f"{self.config.main_table_name}_temp"
        cloned_temp_view = f"{self.config.cloned_table_name}_temp"

        # Create final temporary view for the main table
        self.spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {main_temp_view} AS
            SELECT * FROM {main_temp_stg_view}
            LIMIT {self.config.limit}
        """)

        # Create final temporary view for the cloned table
        self.spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {cloned_temp_view} AS
            SELECT * FROM {cloned_temp_stg_view}
            LIMIT {self.config.limit}
        """)

        logger.info(f"Final temporary tables created: {main_temp_view}, {cloned_temp_view}")

    def drop_initial_temp_tables(self):
        """
        Drop the initial temporary tables with the postfix '_temp_Stg'.
        """
        main_temp_stg_view = f"{self.config.main_table_name}_temp_Stg"
        cloned_temp_stg_view = f"{self.config.cloned_table_name}_temp_Stg"

        self.spark.catalog.dropTempView(main_temp_stg_view)
        self.spark.catalog.dropTempView("keys_temp_Stg")
        self.spark.catalog.dropTempView(cloned_temp_stg_view)

        logger.info(f"Initial temporary tables dropped: {main_temp_stg_view}, keys_temp_Stg, {cloned_temp_stg_view}")

    def show_temp_tables(self):
        """
        Show the first few rows of the final temporary tables for verification.
        """
        main_temp_view = f"{self.config.main_table_name}_temp"
        cloned_temp_view = f"{self.config.cloned_table_name}_temp"

        logger.info(f"First few rows of {main_temp_view}:")
        self.spark.sql(f"SELECT * FROM {main_temp_view} LIMIT 10").show()

        logger.info(f"First few rows of {cloned_temp_view}:")
        self.spark.sql(f"SELECT * FROM {cloned_temp_view} LIMIT 10").show()


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
