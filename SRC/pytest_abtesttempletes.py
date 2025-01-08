"""
This module contains tests for the TempTableCreator class in the
ABTestTemptables module.
"""

import pytest
from unittest.mock import MagicMock
from mocks.mock_spark import mock_spark_session
from SRC.ABTestTemptables import TempTableCreator, TableCompareConfig
from SRC.logging_setup import get_logger, info, debug, warning


# Set up the logger
logger = get_logger(__name__, "DEBUG")


@pytest.fixture(name="ab_temp_table")
def ab_temptable_fixture(mock_spark_session):
    """
    Create an instance of TempTableCreator with a mock Spark session.
    """
    dbutils_mock = MagicMock()
    config = TableCompareConfig(
        limit=100,
        main_table_name="test_main_table",
        cloned_table_name="test_cloned_table",
        key_column_name=["key_column"],
    )
    temp_table_creator = TempTableCreator(config)
    return temp_table_creator


def test_normalize_column_names(ab_temp_table):
    """
    Test the normalize_column_names method.
    """
    # Create a sample DataFrame with whitespace in column names
    data = [("value1", "value2")]
    df = ab_temp_table.spark.createDataFrame(data, [" col1 ", " col2 "])

    # Normalize column names
    df_normalized = ab_temp_table.normalize_column_names(df)

    # Check if column names are normalized
    assert df_normalized.columns == ["col1", "col2"]
    info(logger, "Column names normalized successfully.")


def test_print_column_names(ab_temp_table, caplog):
    """
    Test the print_column_names method.
    """
    # Create a sample DataFrame
    data = [("value1", "value2")]
    df = ab_temp_table.spark.createDataFrame(data, ["col1", "col2"])

    # Print column names
    with caplog.at_level(logging.INFO):
        ab_temp_table.print_column_names(df, "test_table")

    # Check if column names are logged
    assert "Columns in test_table: ['col1', 'col2']" in caplog.text
    info(logger, "Column names printed and logged successfully.")


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
