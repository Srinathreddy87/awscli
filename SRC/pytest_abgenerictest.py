# awscli/SRC/pytest_abgenerictest.py

import pytest
import logging
from unittest.mock import MagicMock, patch
from awscli.SRC.ABGenericScript import ABTestDeltaTables, ABtestconfig
from awscli.SRC.mocks.mock_spark import MockSparkSession, MockDataFrame


# Fixture to set up the ABTestDeltaTables instance with mocks
@pytest.fixture(name="ab_compare")
def ab_compare_fixture():
    dbutils_mock = MagicMock()
    spark_mock = MockSparkSession()
    config = ABtestconfig(
        table_a="test_table_a",
        post_fix="test_post_fix",
        result_table="test_result_table"
    )
    return ABTestDeltaTables(spark_mock, dbutils_mock, config)


# Test the get_schema_from_table method
def test_get_schema_from_table(ab_compare):
    table_name = "test_table"

    # Mock the spark.read.format().table().schema chain
    mock_schema = MagicMock()
    with patch.object(ab_compare.spark.read, 'format', return_value=MagicMock()) as mock_format:
        mock_format.return_value.table.return_value.schema = mock_schema
        result = ab_compare.get_schema_from_table(table_name)
        assert result == mock_schema


# Test the compare_schemas method
def test_compare_schemas(ab_compare, caplog):
    data_a = [("value1", "value2")]
    schema_a = ["key_column1", "key_column2"]
    df_a = MockDataFrame(data_a, schema_a)
    df_a.createOrReplaceTempView = MagicMock()

    data_b = [("value1", "value2")]
    schema_b = ["key_column1", "key_column2"]
    df_b = MockDataFrame(data_b, schema_b)
    df_b.createOrReplaceTempView = MagicMock()

    ab_compare.spark.createDataFrame = MagicMock(side_effect=[df_a, df_b])
    ab_compare.spark.createDataFrame(data_a, schema=schema_a).createOrReplaceTempView("before_table")
    ab_compare.spark.createDataFrame(data_b, schema=schema_b).createOrReplaceTempView("after_table")

    with caplog.at_level(logging.INFO):
        # Mock the spark.read.format().table() chain
        with patch.object(ab_compare.spark.read, 'format', return_value=MagicMock()) as mock_format:
            mock_format.return_value.table.side_effect = [df_a, df_b]
            result = ab_compare.compare_schemas("before_table", "after_table")
            assert result is True

        # Test different schemas
        with patch.object(ab_compare.spark.read, 'format', return_value=MagicMock()) as mock_format:
            mock_format.return_value.table.side_effect = [df_a, MockDataFrame([], ["key_column1"])]
            with pytest.raises(ValueError, match="Schemas are different."):
                ab_compare.compare_schemas("before_table", "after_table")

        # Test different data
        df_b_diff = MockDataFrame([("value3", "value4")], schema_b)
        with patch.object(ab_compare.spark.read, 'format', return_value=MagicMock()) as mock_format:
            mock_format.return_value.table.side_effect = [df_a, df_b_diff]
            with pytest.raises(ValueError, match="Data in tables are different."):
                ab_compare.compare_schemas("before_table", "after_table")


# Test the validate_data method
def test_validate_data(ab_compare):
    data = [("value1", "value2")]
    schema = ["key_column1", "key_column2"]
    df = MockDataFrame(data, schema)
    df.count = MagicMock(return_value=1)  # Mock count method

    data_a = [("value1", "value2")]
    schema_a = ["key_column1", "key_column2"]
    df_a = MockDataFrame(data_a, schema_a)
    df_a.count = MagicMock(return_value=1)  # Mock count method
    df_a.createOrReplaceTempView = MagicMock()

    ab_compare.spark.createDataFrame = MagicMock(return_value=df_a)
    with patch.object(ab_compare.spark.read, 'format', return_value=MagicMock()) as mock_format:
        mock_format.return_value.table.return_value = df_a

        # Assume validate_data returns True if the data is valid
        result = ab_compare.validate_data(df, "after_table")
        assert result is True

        # Test invalid data scenarios
        # Dataframe is None
        with pytest.raises(ValueError, match="Dataframe cannot be None"):
            ab_compare.validate_data(None, "after_table")

        # Dataframe is empty
        df_empty = MockDataFrame([], schema)
        df_empty.count = MagicMock(return_value=0)
        with pytest.raises(ValueError, match="Dataframe is empty"):
            ab_compare.validate_data(df_empty, "after_table")

        # Dataframe missing columns
        df_missing_columns = MockDataFrame(data, ["key_column1"])
        df_missing_columns.count = MagicMock(return_value=1)
        df_missing_columns.columns = ["key_column1"]
        with pytest.raises(ValueError, match="Missing expected column: key_column2"):
            ab_compare.validate_data(df_missing_columns, "after_table")

        # Dataframe and after_table data are different
        df_diff = MockDataFrame([("value3", "value4")], schema)
        mock_format.return_value.table.return_value = df_diff
        with pytest.raises(ValueError, match="Data in dataframe and after_table are different."):
            ab_compare.validate_data(df, "after_table")


if __name__ == "__main__":
    pytest.main()
