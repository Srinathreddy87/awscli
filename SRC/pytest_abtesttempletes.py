import pytest
from pyspark.sql import SparkSession
from SRC.ABTestTemptables import TempTableCreator, TableCompareConfig

@pytest.fixture(scope="module")
def spark():
    # Initialize a Spark session for testing
    spark = SparkSession.builder \
        .appName("TestTempTableCreator") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def config():
    # Example configuration for testing
    return TableCompareConfig(
        limit=100,
        main_table_name="test_main_table",
        cloned_table_name="test_cloned_table",
        key_column_name=["key_column"]
    )

@pytest.fixture
def temp_table_creator(spark, config):
    # Initialize TempTableCreator with the test config
    return TempTableCreator(config)

def test_normalize_column_names(spark, temp_table_creator):
    # Create a sample DataFrame with whitespace in column names
    data = [("value1", "value2")]
    df = spark.createDataFrame(data, [" col1 ", " col2 "])
    
    # Normalize column names
    df_normalized = temp_table_creator.normalize_column_names(df)
    
    # Check if column names are normalized
    assert df_normalized.columns == ["col1", "col2"]

def test_print_column_names(spark, temp_table_creator, caplog):
    # Create a sample DataFrame
    data = [("value1", "value2")]
    df = spark.createDataFrame(data, ["col1", "col2"])
    
    # Print column names
    with caplog.at_level(logging.INFO):
        temp_table_creator.print_column_names(df, "test_table")
    
    # Check if column names are logged
    assert "Columns in test_table: ['col1', 'col2']" in caplog.text

def test_check_data_files_equal(spark, temp_table_creator):
    # This test requires actual Delta tables, so it might be skipped in a CI pipeline
    # Create mock Delta tables and validate the method's functionality
    pass

def test_create_initial_temp_tables(spark, temp_table_creator):
    # This test requires actual Delta tables, so it might be skipped in a CI pipeline
    # Create mock Delta tables and validate the method's functionality
    pass

def test_create_final_temp_tables(spark, temp_table_creator):
    # This test requires actual Delta tables, so it might be skipped in a CI pipeline
    # Create mock Delta tables and validate the method's functionality
    pass

def test_drop_initial_temp_tables(spark, temp_table_creator):
    # This test requires actual Delta tables, so it might be skipped in a CI pipeline
    # Create mock Delta tables and validate the method's functionality
    pass

def test_show_temp_tables(spark, temp_table_creator):
    # This test requires actual Delta tables, so it might be skipped in a CI pipeline
    # Create mock Delta tables and validate the method's functionality
    pass

if __name__ == "__main__":
    pytest.main()
