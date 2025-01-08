"""
This module contains tests for the ABTestDeltaTables class in the
ABGenericScript module.
"""

import pytest
from unittest.mock import MagicMock
from mocks.mock_spark import mock_spark_session
from SRC.ABGenericScript import ABTestDeltaTables, ABTestConfig
from SRC.logging_setup import get_logger, info

# Set up the logger
logger = get_logger(__name__, "DEBUG")


@pytest.fixture(name="ab_test")
def ab_test_fixture(mock_spark_session):
    """
    Create an instance of ABTestDeltaTables with a mock Spark session.
    """
    dbutils_mock = MagicMock()
    config = ABTestConfig(
        table_a="test_table_a",
        table_b="test_table_b",
        result_table="test_result_table",
        key_columns=["key_column1", "key_column2"],
    )
    ab_test = ABTestDeltaTables(config)
    ab_test.spark = mock_spark_session
    return ab_test


def test_compare_schemas(ab_test, caplog):
    """
    Test the compare_schemas method.
    """
    # Create sample DataFrames for table_a and table_b
    data_a = [("value1", "value2")]
    schema_a = ["key_column1", "key_column2"]
    df_a = ab_test.spark.createDataFrame(data_a, schema_a)
    df_a.write.format("delta").mode("overwrite").saveAsTable("test_table_a")

    data_b = [("value1", "value2")]
    schema_b = ["key_column1", "key_column2"]
    df_b = ab_test.spark.createDataFrame(data_b, schema_b)
    df_b.write.format("delta").mode("overwrite").saveAsTable("test_table_b")

    # Compare schemas
    with caplog.at_level(logging.INFO):
        ab_test.compare_schemas()

    # Check if schemas are logged as identical
    assert "Schemas are identical." in caplog.text
    info(logger, "Schemas compared successfully.")


def test_validate_data(ab_test):
    """
    Test the validate_data method.
    """
    # Create sample DataFrames for table_a and table_b
    data_a = [(1, "value1_a", "value2_a")]
    schema_a = ["key_column1", "key_column2", "column_a"]
    df_a = ab_test.spark.createDataFrame(data_a, schema_a)
    df_a.write.format("delta").mode("overwrite").saveAsTable("test_table_a")

    data_b = [(1, "value1_b", "value2_b")]
    schema_b = ["key_column1", "key_column2", "column_b"]
    df_b = ab_test.spark.createDataFrame(data_b, schema_b)
    df_b.write.format("delta").mode("overwrite").saveAsTable("test_table_b")

    # Validate data
    ab_test.validate_data()

    # Check if the result table is created
    result_df = ab_test.spark.read.format("delta").table("test_result_table")
    assert result_df.count() > 0
    info(logger, "Data validated successfully.")


def test_construct_unmatched_query(ab_test):
    """
    Test the construct_unmatched_query method.
    """
    # Construct the unmatched query
    query = ab_test.construct_unmatched_query()
    expected_conditions = (
        "key_column1_a IS NULL OR key_column1_b IS NULL OR "
        "key_column1_a != key_column1_b OR key_column2_a IS NULL OR "
        "key_column2_b IS NULL OR key_column2_a != key_column2_b"
    )
    expected_query = f"""
        SELECT *,
        CASE
            WHEN {expected_conditions} THEN 'unmatched'
            ELSE 'matched'
        END AS validation_result
        FROM joined_view
        WHERE {expected_conditions}
    """
    assert query.strip() == expected_query.strip()
    info(logger, "Unmatched query constructed successfully.")


if __name__ == "__main__":
    pytest.main()
