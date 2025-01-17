# awscli/SRC/pytest_abgenerictest.py

import pytest
import logging
from unittest.mock import MagicMock, patch
from awscli.SRC.ABGenericScript import ABTestDeltaTables, ABtestconfig
from awscli.SRC.mocks.mock_spark import MockSparkSession, MockDataFrame


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

    with patch('awscli.SRC.ABGenericScript.Read.load', return_value="database.schema"):
        result = ab_compare.get_schema_from_table(table_name)
        assert result == "database.schema"


def test_compare_schemas(ab_compare, caplog):
    # Mocked data and schema
    data_a = [("value1", "value2")]
    schema_a = ["key_column1", "key_column2"]
    df_a = MockDataFrame(data_a, schema_a)
    df_a.createOrReplaceTempView = MagicMock()

    data_b = [("value1", "value2")]
    schema_b = ["key_column1", "key_column2"]
    df_b = MockDataFrame(data_b, schema_b)
    df_b.createOrReplaceTempView = MagicMock()

    # Mocking spark.createDataFrame
    ab_compare.spark.createDataFrame = MagicMock(side_effect=[df_a, df_b])

    # Creating temporary views
    ab_compare.spark.createDataFrame(data_a, schema=schema_a).createOrReplaceTempView("before_table")
    ab_compare.spark.createDataFrame(data_b, schema=schema_b).createOrReplaceTempView("after_table")

    # Run the compare_schemas method with logging
    with caplog.at_level(logging.INFO):
        ab_compare.compare_schemas("before_table", "after_table")

    # Assert the correct log message is present
    assert "Schemas are identical." in caplog.text, f"Expected log message not found. Logs: {caplog.text}"

if __name__ == "__main__":
    pytest.main()
