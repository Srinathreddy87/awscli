"""
This module contains tests for the ABTestDeltaTables class in the ABGenericScript module.
"""

import logging
from unittest.mock import MagicMock

import pytest

from mocks.mock_spark import mock_spark_session
from sparta.abtest import ABTestConfig, ABTestDeltaTables
from sparta.common_logger import get_logger, info

# Set up the logger
logger = get_logger(__name__, "DEBUG")

@pytest.fixture(name="ab_test")
def ab_test_fixture():
    """
    Create an instance of ABTestDeltaTables with a mock Spark session.
    """
    config = ABTestConfig(
        table_a="test_table_a",
        post_fix="test_post_fix",
        result_table="test_result_table",
        key_columns=["key_column1", "key_column2"],
    )
    ab_test = ABTestDeltaTables(mock_spark_session(), MagicMock(), config)
    return ab_test

def test_compare_schemas(ab_test, caplog):
    """
    Test the compare_schemas method.
    """
    # Create sample DataFrames for before_table and after_table
    data_a = [("value1", "value2")]
    schema_a = ["key_column1", "key_column2"]
    df_a = ab_test.spark.createDataFrame(data_a, schema=schema_a)
    ab_test.spark.registerDataFrameAsTable(df_a, "before_table")

    data_b = [("value1", "value2")]
    schema_b = ["key_column1", "key_column2"]
    df_b = ab_test.spark.createDataFrame(data_b, schema=schema_b)
    ab_test.spark.registerDataFrameAsTable(df_b, "after_table")

    # Compare schemas
    with caplog.at_level(logging.INFO):
        ab_test.compare_schemas("before_table", "after_table")

    # Check if schemas are logged as identical
    assert "Schemas are identical." in caplog.text
    info(logger, "Schemas compared successfully.")

def test_construct_unmatched_query(ab_test):
    """
    Construct the unmatched query.
    """
    query = ab_test.construct_unmatched_query()
    expected_query = f"""
    SELECT
        CASE
            WHEN {expected_conditions} THEN 'unmatched'
            ELSE 'matched'
        END AS validation_result
    FROM joined_view
    WHERE {expected_conditions};
    """

    # Normalize whitespace for comparison
    def normalize_whitespace(query):
        return " ".join(query.split())

    assert normalize_whitespace(query) == normalize_whitespace(expected_query)
