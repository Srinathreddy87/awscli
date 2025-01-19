import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from sparta import ABTestDeltaTables  # Assuming the class is in sparta.py

@pytest.fixture
def mock_spark():
    """Fixture to provide a mock SparkSession with simplified mocks."""
    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = MagicMock()
    return mock_spark

@patch('sparta.ABTestDeltaTables.spark', new_callable=mock_spark)
@patch('sparta.datetime')  # Patch datetime to control the run_date
def test_prepare_results(mock_datetime, mock_spark):
    """Test the prepare_results method."""
    ab_test = ABTestDeltaTables(mock_spark)
    mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)  # Fixed datetime
    mock_comparison_df = MagicMock()
    mock_comparison_df.filter.return_value.count.return_value = 5  # Mock count
    columns = ['col1_a', 'col2_a', 'col3_a']  # Sample columns

    results = ab_test.prepare_results(mock_comparison_df, columns, "before_table", "after_table")

    assert len(results) == len(columns)
    for i, col in enumerate(columns):
        assert results[i] == {
            "test_name": "ABTest",
            "table_a": "before_table",
            "table_b": "after_table",
            "column_name": col.replace("_a", ""),
            "schema_mismatch": False,  # Assuming all columns exist
            "data_mismatch": True,  # Since count is > 0
            "mismatch_count": 5,
            "validation_errors": None,
            "run_date": datetime(2024, 1, 1, 12, 0, 0)
        }

    # Check filter condition
    mock_comparison_df.filter.assert_called_with("col1_result = 'unmatch'")

@patch('sparta.ABTestDeltaTables.spark', new_callable=mock_spark)
def test_write_results(mock_spark):
    """Test the write_results method."""
    ab_test = ABTestDeltaTables(mock_spark)
    mock_results_df = MagicMock()

    ab_test.write_results(mock_results_df, "audit_table_name")

    # Assertions
    mock_results_df.write.format.assert_called_with("delta")
    mock_results_df.write.format().mode.assert_called_with("append")
    mock_results_df.write.format().mode().saveAsTable.assert_called_once_with("audit_table_name")
    # You'll need to add assertions to check for logging messages using caplog fixture, similar to previous examples
