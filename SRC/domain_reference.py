"""
The domain_reference module contains helpers for creating data frames.
"""

import logging
import os
import re
from string import Formatter

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession

class DomainReference:
    """
    DomainReference provides Data Frame abstraction layer for SPARTA.

    This class is used in Notebooks to create Data Frames and Execute SQL.
    """

    INVALID_SQL_ERROR = "Invalid SQL Expression"
    INVALID_TABLE_LIST = "Table list was not initialized."
    DOMAIN_REFERENCE = "domain_reference"
    TABLE_REFERENCE = "table_reference"
    ENTERPRISE_DATA_LAKE = "en_dlake"
    DEFAULT_ENV = "dev"
    EV_ENV = "ENV"
    EV_WORK_SCHEMA = "WORK_SCHEMA"

    dbutils = None
    environment = ""
    domain_table = ""
    table_reference_table_path = ""
    logger = logging.getLogger()
    # Holds instance-level data frame from table reference table
    table_reference = None
    work_schema = ""

    def _get_env(self) -> str:
        return os.getenv(self.EV_ENV, self.DEFAULT_ENV)

    def _get_domain_table_path(self) -> str:
        return (
            f"{self.ENTERPRISE_DATA_LAKE}/{self.environment}/"
            f"{self.work_schema}.{self.DOMAIN_REFERENCE}"
        )

    def _get_table_reference_table_path(self) -> str:
        return (
            f"{self.ENTERPRISE_DATA_LAKE}/{self.environment}/"
            f"{self.work_schema}.{self.TABLE_REFERENCE}"
        )


    def _get_table_reference_table_path(self) -> str:
        return (
            f"{self.ENTERPRISE_DATA_LAKE}/{self.environment}/"
            f"{self.work_schema}.{self.TABLE_REFERENCE}"
        )

    def _get_work_schema_path(self) -> str:
        work_schema = os.getenv(self.EV_WORK_SCHEMA)
        if work_schema == "":
            self.logger.warning("work_schema not set.")
        return f"{work_schema}"

    def __init__(self, spark: SparkSession, dbutils):
        """
        Initialize the class with a Spark session and a dbutils instance.

        Parameters:
        - spark: SparkSession instance. If not provided, error will be thrown.
        - dbutils: Pass the Global singleton provided by DBx.
        """
        self.spark = spark
        self.dbutils = dbutils
        self.work_schema = self._get_work_schema_path()
        self.environment = self._get_env()
        self.domain_table = self._get_domain_table_path()
        self.table_reference_table_path = (
            self._get_table_reference_table_path()
        )

        self.logger.info("Initializing DomainReference")
        self.logger.info("Environment: %s", self.environment)
        self.logger.info("Domain Table: %s", self.domain_table)

    def _get_sql_from_domain_ref(self, domain_reference_key) -> str:
        result_df = self.spark.sql(
            f"""
            SELECT domain_value
            FROM en_dlake_dev.pcdm_work.domain_reference
            WHERE domain_cd = '{domain_reference_key}'
            """
        )
        return f"{result_df.first()[0]}"

    def create_data_frame_from_ref_key(
        self, domain_reference_key
    ) -> DataFrame:
        """
        Create a data frame from a record in the Domain Reference Table.
        """
        # Implementation details go here.
        
    def resolve_table_name(self, table_key):
         """
         A
         
         
         
         
         
         
         Args :
         table_key    
    
    
        """
        try:
        	matching_string = self.spark.sql(
        	f"""
        SELECT table_path, stage_table_path
        FROM {self.table_reference_table_path}
        WHERE table_cd = '{table_key}'
        """
        )
            matching_row = matching_table.first()

            if self.is_staging_file():
                return matching_row["stage_table_path"]
            return matching_row["table_path"]

        except Py4JJavaError:
            self.logger.critical(
                "No matching table code matching %s, was found.",
                table_key,
                exc_info=True
            )
            return None

        except AttributeError:
            self.logger.critical(
                "Unexpected error when looking up %s.",
                table_key,
                exc_info=True
            )
            return None

    def is_staging_file(self) -> bool:
        """
        Return true if the current running file is a staging file.

        Uses naming convention where staging files end in _stage
        Returns True if a staging file, otherwise False.
        """
        if not self.dbutils:
            raise ValueError(
                "The required dbutils instance was not initialized"
            )

        notebook_name = (
            self.dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )

        return notebook_name.endswith("_stage.py")

    def map_table_variables(self, tables) -> dict:
        """
        Look up paths for a list of table keys and return a map.

        Args:
            tables (list): List of table keys

        Returns:
            map: Returns a list of table keys and associated paths
        """
        # While empty list is permitted, a None value
        # will indicate an issue with the upstream method
        # Throw exception to prevent process from continuing
        if tables is None:
            raise ValueError(self.INVALID_TABLE_LIST)
       
        table_map = {var: self.resolve_table_name(var) for var in tables}
        return table_map

    def extract_table_keys(self, sql_string) -> list:
        """
        Extract all table keys from the given string.

        Parses the string and extracts all substrings
        that match the table_key pattern tbl_some_name.

        Args:
            sql_string (String): SQL statement with variables

        Returns:
            list of str: A list of unique variables and resolved table names
        """
        # Raise an exception when sql_string is empty
        if sql_string is None or sql_string == "":
            raise ValueError(self.INVALID_SQL_ERROR)

        # Regular expression to match variables starting with tbl_
        pattern = re.compile(r"\b(tbl_[\w\d_]+)\b")

        # Return a list of matches with prefix removed
        return [var[4:] for var in set(pattern.findall(sql_string))]



class DomainReference:
    def _safe_format(self, template: str) -> str:
        """
        Filter out variables that are not defined to prevent key errors.
        """
        safe_vars = {
            key: os.environ.get(key, f"{{{{{key}}}}}") for key in placeholders
        }
        return template.format(**safe_vars)

    def render_sql(self, table_map: dict, sql_template: str) -> str:
        """
        Convert SQL template into a valid SQL string.

        Args:
            table_map (map): Map of table keys and table paths.
            sql_template (str): SQL statement with variables.

        Returns:
            str: SQL string with variables replaced with values.
        """
        rendered_sql = sql_template
        for table_key, table_path in table_map.items():
            pattern = re.compile(rf"\{{\{{\s*tbl_{table_key}\s*\}}\}}")
            rendered_sql = re.sub(pattern, table_path, rendered_sql)

        rendered_sql = self._safe_format(rendered_sql)
        return rendered_sql

    def execute_sql(self, domain_reference_key):
        """
        Execute a SQL command but does not return a data frame.

        Args:
            domain_reference_key (String): Key for looking up the query.

        Returns:
            boolean: Returns true if the query was successful, otherwise false.
        """
        sql = self._get_sql_from_domain_ref(domain_reference_key)
        keys = self._extract_table_keys(sql)
        table_map = self.map_table_variables(keys)
        rendered_sql = self.render_sql(table_map, sql)
        self.spark.sql(rendered_sql)

    def get_table_config(self, table_key):
        """
        Get the table configuration from the table reference table.

        Args:
            table_key (String): The key for the table you would like to find.
        """
        # Implementation details for getting table configuration.
