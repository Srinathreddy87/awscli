"""
This module contains tests for the TempTableCreator class in the
ABTestTemptables module.
"""

import logging
import pytest
from unittest.mock import MagicMock
from mocks.mock_spark import mock_spark_session
from SRC.ABTestTemptables import TempTableCreator, TableCompareConfig


@pytest.fixture(name="ab_temp_table")
def ab_temptable_fixture(mock_spark_session):
    """
    Create an instance of TempTableCreator with a mock Spark session.
    """
    dbutils_mock = MagicMock()
    temp_table_creator = TempTableCreator(mock_spark_session, dbutils_mock)
    table_compare_config = TableCompareConfig(mock_spark_session, dbutils_mock)
    return temp_table_creator, table_compare_config


def test_normalize_column_names(ab_temp_table):
    """
    Test the normalize_column_names method.
    """
    temp_table_creator, _ = ab_temp_table
    # Create a sample DataFrame with whitespace in column names
    data = [("value1", "value2")]
    df = temp_table_creator.spark.createDataFrame(data, [" col1 ", " col2 "])

    # Normalize column names
    df_normalized = temp_table_creator.normalize_column_names(df)

    # Check if column names are normalized
    assert df_normalized.columns == ["col1", "col2"]


def test_print_column_names(ab_temp_table, caplog):
    """
    Test the print_column_names method.
    """
    temp_table_creator, _ = ab_temp_table
    # Create a sample DataFrame
    data = [("value1", "value2")]
    df = temp_table_creator.spark.createDataFrame(data, ["col1", "col2"])

    # Print column names
    with caplog.at_level(logging.INFO):
        temp_table_creator.print_column_names(df, "test_table")

    # Check if column names are logged
    assert "Columns in test_table: ['col1', 'col2']" in caplog.text


def test_check_data_files_equal(ab_temp_table):
    """
    This test requires actual Delta tables,
    so it might be skipped in a CI pipeline.
    Create mock Delta tables and validate the method's functionality.
    """
    pass


def test_create_initial_temp_tables(ab_temp_table):
    """
    This test requires actual Delta tables,
    so it might be skipped in a CI pipeline.
    Create mock Delta tables and validate the method's functionality.
    """
    pass


def test_create_final_temp_tables(ab_temp_table):
    """
    This test requires actual Delta tables,
    so it might be skipped in a CI pipeline.
    Create mock Delta tables and validate the method's functionality.
    """
    pass


def test_drop_initial_temp_tables(ab_temp_table):
    """
    This test requires actual Delta tables,
    so it might be skipped in a CI pipeline.
    Create mock Delta tables and validate the method's functionality.
    """
    pass


def test_show_temp_tables(ab_temp_table):
    """
    This test requires actual Delta tables,
    so it might be skipped in a CI pipeline.
    Create mock Delta tables and validate the method's functionality.
    """
    pass


if __name__ == "__main__":
    pytest.main()
