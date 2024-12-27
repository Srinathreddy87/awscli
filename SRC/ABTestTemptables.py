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

    def normalize_column_names(self, df):
        """
        Normalize column names by trimming whitespace.
        """
        for col in df.columns:
            df = df.withColumnRenamed(col, col.strip())
        return df

    def print_column_names(self, df, table_name):
        """
        Print the column names of a DataFrame.
        """
        logger.info(f"Columns in {table_name}: {df.columns}")

    def check_data_files_equal(self, table_a, table_b):
        """
        Check if the data files for two tables are equal.
        """
        df_a = self.spark.sql(f"DESCRIBE DETAIL {table_a}")
        df_b = self.spark.sql(f"DESCRIBE DETAIL {table_b}")

        files_a = df_a.select("partitionColumns", "numFiles", "sizeInBytes").collect()
        files_b = df_b.select("partitionColumns", "numFiles", "sizeInBytes").collect()

        return files_a == files_b

    def recreate_cloned_table(self):
        """
        Recreate the cloned table.
        """
        self.spark.sql(f"DROP TABLE IF EXISTS {self.config.cloned_table_name}")
        self.spark.sql(f"CREATE TABLE {self.config.cloned_table_name} AS SELECT * FROM {self.config.main_table_name}")
        logger.info(f"Cloned table '{self.config.cloned_table_name}' recreated")

    def create_initial_temp_tables(self):
        """
        Create initial temporary tables with the postfix '_temp_Stg'.
        """
        # Load main table and normalize column names
        df_main = self.spark.read.format("delta").table(self.config.main_table_name)
        df_main = self.normalize_column_names(df_main)
        
        main_temp_stg_table = f"{self.config.main_table_name}_temp_Stg"
        df_main.write.mode("overwrite").saveAsTable(main_temp_stg_table)
        logger.info(f"Main temporary table '{main_temp_stg_table}' created")

        # Print column names for debugging
        self.print_column_names(df_main, "main_temp_stg_table")

        # Select distinct keys from the main temporary table
        key_columns = self.config.key_column_name
        key_columns_str = ", ".join(key_columns)
        key_table_name = f"key_{self.config.main_table_name}"
        df_keys = self.spark.sql(f"""
            SELECT DISTINCT {key_columns_str}
            FROM {main_temp_stg_table}
            LIMIT {self.config.limit}
        """)
        df_keys.write.mode("overwrite").saveAsTable(key_table_name)
        logger.info(f"Keys temporary table '{key_table_name}' created")

        # Print column names for debugging
        self.print_column_names(df_keys, "key_table_name")

        # Load cloned table and normalize column names
        df_cloned = self.spark.read.format("delta").table(self.config.cloned_table_name)
        df_cloned = self.normalize_column_names(df_cloned)
        
        cloned_temp_stg_table = f"{self.config.cloned_table_name}_temp_Stg"
        
        # Print column names for debugging
        self.print_column_names(df_cloned, "cloned_temp_stg_table")

        # Generate the join condition
        join_condition = [df_cloned[col] == df_keys[col] for col in key_columns]

        # Perform the join and select all columns from the cloned table
        df_cloned_filtered = df_cloned.join(df_keys, join_condition, "inner").select(df_cloned["*"])
        
        # Debugging: Print schemas and sample data
        logger.info("Schema of main temporary table:")
        df_main.printSchema()
        logger.info("Sample data from main temporary table:")
        df_main.show(10)

        logger.info("Schema of keys temporary table:")
        df_keys.printSchema()
        logger.info("Sample data from keys temporary table:")
        df_keys.show(10)

        logger.info("Schema of cloned table:")
        df_cloned.printSchema()
        logger.info("Sample data from cloned table:")
        df_cloned.show(10)

        logger.info("Schema of cloned filtered table:")
        df_cloned_filtered.printSchema()
        logger.info("Sample data from cloned filtered table:")
        df_cloned_filtered.show(10)

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

    # Check if data files are equal and recreate the cloned table if not
    if not temp_table_creator.check_data_files_equal(config.main_table_name, config.cloned_table_name):
        temp_table_creator.recreate_cloned_table()

    # Create initial, final temp tables and show the result
    temp_table_creator.create_initial_temp_tables()
    temp_table_creator.create_final_temp_tables()
    temp_table_creator.drop_initial_temp_tables()
    temp_table_creator.show_temp_tables()


------------------------

Hi Mahantesh and Vishal,

I wanted to provide an update on the AB framework. Initially, we had targeted completion by 01/03/2025. However, due to a shortage of resources, progress has not been as expected. The new expected ETA is on or before 01/10/2025.

As of now, I have created two scripts according to the client's requirements and completed initial testing.

Pending Tasks:

Format both scripts according to Black format and Python re-commit version 11.
Enhance the scripts to add logic that validates the clone table and source table, ensuring both are in sync.
Add logic to support scripting for curated tables.
Create a common function to be used across projects.
Develop a dashboard to display AB testing results.
Considering all the tasks, I expect to complete them by 01/10/2025.
