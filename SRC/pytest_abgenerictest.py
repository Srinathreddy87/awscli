import pytest
import pandas as pd
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

# Test the validate_data method
def test_validate_data(ab_compare):
    schema = ["key_column1", "key_column2"]
    data = [("value1", "value2")]
    df = MockDataFrame(pd.DataFrame(data, columns=schema))
    df.count = MagicMock(return_value=1)  # Mock count method

    df_a = MockDataFrame(pd.DataFrame(data, columns=schema))
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
        df_empty = MockDataFrame(pd.DataFrame([], columns=schema))
        df_empty.count = MagicMock(return_value=0)
        with pytest.raises(ValueError, match="Dataframe is empty"):
            ab_compare.validate_data(df_empty, "after_table")

        # Dataframe missing columns
        df_missing_columns = MockDataFrame(pd.DataFrame(data, columns=["key_column1"]))
        df_missing_columns.count = MagicMock(return_value=1)
        with pytest.raises(ValueError, match="Missing expected column: key_column2"):
            ab_compare.validate_data(df_missing_columns, "after_table")

        # Dataframe and after_table data are different
        df_diff = MockDataFrame(pd.DataFrame([("value3", "value4")], columns=schema))
        mock_format.return_value.table.return_value = df_diff
        with pytest.raises(ValueError, match="Data in dataframe and after_table are different."):
            ab_compare.validate_data(df, "after_table")

        # Handle FileNotFoundError scenario
        with patch.object(ab_compare.spark.read, 'table', side_effect=FileNotFoundError("File not found: before_table")):
            with pytest.raises(FileNotFoundError, match="File not found: before_table"):
                ab_compare.validate_data(df, "before_table")

if __name__ == "__main__":
    pytest.main()
