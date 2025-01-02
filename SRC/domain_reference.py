"""
This module contains helpers for creating DataFrames,
including initializing a Spark session and using dbutils.
"""

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

class SparkHelper:
    """
    A helper class to initialize Spark session and create DataFrames.
    """

    @staticmethod
    def get_spark_session(app_name: str = "DefaultApp") -> SparkSession:
        """
        Get or create a Spark session.

        :param app_name: The name of the Spark application.
        :return: A Spark session object.
        """
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        return spark

    @staticmethod
    def get_dbutils(spark: SparkSession) -> DBUtils:
        """
        Get the dbutils object from the Spark session.

        :param spark: A Spark session object.
        :return: A DBUtils object.
        """
        return DBUtils(spark)

    @staticmethod
    def create_dataframe(spark: SparkSession, data: list, schema: list = None):
        """
        Create a DataFrame from a list of data.

        :param spark: A Spark session object.
        :param data: A list of data for the DataFrame.
        :param schema: An optional schema for the DataFrame.
        :return: A DataFrame object.
        """
        if schema:
            return spark.createDataFrame(data, schema)
        return spark.createDataFrame(data)

# Example usage
if __name__ == "__main__":
    spark = SparkHelper.get_spark_session("ExampleApp")
    dbutils = SparkHelper.get_dbutils(spark)
    data = [(1, "Alice"), (2, "Bob")]
    schema = ["id", "name"]
    df = SparkHelper.create_dataframe(spark, data, schema)
    df.show()
