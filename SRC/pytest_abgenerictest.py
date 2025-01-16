# SRC/pytest_abgenerictest.py

import pytest
from unittest.mock import MagicMock
from SRC.mocks.mock_spark import MockSparkSession, MockDataFrame

# Import your ABGenericScript module here
# from SRC.ABGenericScript import YourFunctionOrClass

@pytest.fixture(name = "ab_compare")
def ab_compare_fixture():
    dbuitls_mock = MagicMock()
    spark_mock = mock_spark_session()
    config = ABtestconfig(
        table_a="test_table_a",
        post_fix = "test_post_fix",
        result_table = "test_result_table"
    )
    retrun ABTestDeltaTables(spark_mock, dbuitls_mock, config)
def tes_determine_get_schema_rom_table(
    ab_compare,
):
    table_compare_config = MagicMock()
    table_compare_config.table_name = "test_table"
    table_name = "test_table"
    result = ab_compare.get_schema_from_table(table_compare_config, table_name)
    assert results == "database.schema"
