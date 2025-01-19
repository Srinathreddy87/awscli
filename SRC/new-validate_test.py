import pytest
from unittest.mock import patch, MagicMock
from sparta import ABTestDeltaTables  # Assuming the class is in sparta.py

@pytest.fixture
def mock_spark():
    """Fixture to provide a mock SparkSession with simplified mocks."""
    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = MagicMock()
    return mock_spark

@patch('sparta.ABTestDeltaTables.spark', new_callable=mock_spark)
def test_rename_columns(mock_spark):
    """Test the rename_columns method."""
    ab_test = ABTestDeltaTables(mock_spark)
    mock_df = MagicMock(columns=['col1', 'col2'])
    renamed_df = ab_test.rename_columns(mock_df, "_a")

    # Assertions
    assert renamed_df.withColumnRenamed.call_count == len(mock_df.columns)
    for col in mock_df.columns:
        renamed_df.withColumnRenamed.assert_any_call(col, f"{col}_a")

@patch('sparta.ABTestDeltaTables.spark', new_callable=mock_spark)
def test_create_join_condition(mock_spark):
    """Test the create_join_condition method."""
    ab_test = ABTestDeltaTables(mock_spark)
    mock_df1 = MagicMock(columns=['col1', 'col2'])
    mock_df2 = MagicMock(columns=['col1', 'col2'])
    join_condition = ab_test.create_join_condition(mock_df1, mock_df2)

    # Assertions
    assert join_condition == "df1.col1 = df2.col1 AND df1.col2 = df2.col2"

@patch('sparta.ABTestDeltaTables.spark', new_callable=mock_spark)
def test_construct_comparison_query(mock_spark):
    """Test the construct_comparison_query method."""
    ab_test = ABTestDeltaTables(mock_spark)
    mock_df_a = MagicMock(columns=['col1_a', 'col2_a'])
    mock_df_b = MagicMock(columns=['col1_b', 'col2_b'])
    query = ab_test.construct_comparison_query(mock_df_a, mock_df_b)

    # Assertions
    expected_query = """
    SELECT col1_a, col2_a, col1_b, col2_b,
    CASE WHEN col1_a IS NULL OR col1_b IS NULL THEN 'unmatch'
         WHEN col1_a = col1_b THEN 'match'
         ELSE 'unmatch' END AS col1_result,
    CASE WHEN col2_a IS NULL OR col2_b IS NULL THEN 'unmatch'
         WHEN col2_a = col2_b THEN 'match'
         ELSE 'unmatch' END AS col2_result,
    CASE WHEN col1_result = 'unmatch' OR col2_result = 'unmatch'
         THEN 'unmatch'
         ELSE 'match' END AS validation_result
    FROM joined_view
    """
    assert query.strip() == expected_query.strip()
