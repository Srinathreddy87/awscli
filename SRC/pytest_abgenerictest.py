# awscli/SRC/pytest_abgenerictest.py

import pytest
import logging
from unittest.mock import MagicMock, patch
from awscli.SRC.ABGenericScript import ABTestDeltaTables, ABtestconfig
from awscli.SRC.mocks.mock_spark import MockSparkSession


@pytest.fixture(name="ab_compare")
def ab_compare_fixture():
    """Fixture to set up the ABTestDeltaTables instance with mocks."""
    dbutils_mock = MagicMock()
    spark_mock = MockSparkSession()
    config = ABtestconfig(
        table_a="test_table_a",
        post_fix="test_post_fix",
        result_table="test_result_table"
    )
    return ABTestDeltaTables(spark_mock, dbutils_mock, config)


def test_get_schema_from_table(ab_compare):
    """Test the get_schema_from_table method."""
    table_name = "test_table"

    with patch('awscli.SRC.ABGenericScript.Read.format', return_value="database.schema"):
        result = ab_compare.get_schema_from_table(table_name)
        assert result == "database.schema"


def test_compare_schemas(ab_compare, caplog):
    """Test the compare_schemas method."""
    data_a = [("value1", "value2")]
    schema_a = ["key_column1", "key_column2"]
    df_a = ab_compare.spark.createDataFrame(data_a, schema=schema_a)
    df_a.createOrReplaceTempView("before_table")
    
    data_b = [("value1", "value2")]
    schema_b = ["key_column1", "key_column2"]
    df_b = ab_compare.spark.createDataFrame(data_b, schema=schema_b)
    df_b.createOrReplaceTempView("after_table")
    
    with caplog.at_level(logging.INFO):
        ab_compare.compare_schemas("before_table", "after_table")
    
    assert "Schemas are identical." in caplog.text


if __name__ == "__main__":
    pytest.main()


# In awscli/SRC/mocks/mock_spark.py

class MockDataFrame:
    def __init__(self, data, schema):
        self.data = data
        self.schema = schema
    
    def createOrReplaceTempView(self, name):
        """Mock method for createOrReplaceTempView"""
        print(f"Mock createOrReplaceTempView called with name: {name}")

class MockSparkSession:
    def createDataFrame(self, data, schema):
        """Mock method for createDataFrame"""
        return MockDataFrame(data, schema)

    def registerDataFrameAsTable(self, df, name):
        """Mock method for registerDataFrameAsTable"""
        df.createOrReplaceTempView(name)
