
"""Mock that emulates some Spark Functionality."""

import logging
import os
import re
from unittest.mock import MagicMock

import pandas as pd
import sqlparse
from py4j.protocol import Py4JJavaError

FIXTURES_DIR = "fixtures"

class NoSuchElementException(Exception):
    """Mock Java exception."""

# Create Spark alias for pandas functions
# pylint: disable-msg=C0103
def asDict(self):
    """Emulate Spark asDict function."""
    return self.to_dict()

pd.Series.asDict = asDict

class MockDataFrame:
    """Class to mock Spark DataFrame using Pandas to emulate."""

    def __init__(self, pandas_df: pd.DataFrame, columns=None):
        # Initialize Mock DataFrame
        if isinstance(pandas_df, pd.DataFrame):
            self.pandas_df = pandas_df
        else:
            self.pandas_df = pd.DataFrame(pandas_df, columns)

    @property
    def columns(self):
        """Return data frame columns as a list."""
        return self.pandas_df.columns.to_list()

    def show(self, n=20):
        """Display first n number of rows in a DataFrame."""
        print(self.pandas_df.head(n))

    def filter(self, condition):
        """Simulate Spark DataFrame's filter using Pandas filtering."""
        return MockDataFrame(self.pandas_df.query(condition))

    def select(self, *cols):
        """Simulate Spark DataFrame's select using Pandas df[cols]."""
        return MockDataFrame(self.pandas_df[list(cols)])

    # pylint: disable-msg=C0103
    def toPandas(self):
        """Simulate Spark DataFrame's toPandas by returning self."""
        return self.pandas_df

    def collect(self):
        """Simulate DataFrame's collect by returning rows as list of tuples."""
        return [tuple(row) for row in self.pandas_df.itertuples(index=False)]

    def count(self):
        """Simulate DataFrame count using len on Pandas DataFrame."""
        return len(self.pandas_df)

    def head(self, num=5):
        """Simulate Spark head function."""
        return self.pandas_df.head(num)

    def first(self):
        """Return first row of the data frame."""
        if self.pandas_df.empty:
            raise Py4JJavaError(
                "Py4JJavaError(NoSuchElementException)",
                NoSuchElementException,
            )
        return self.pandas_df.iloc[0]

    def isEmpty(self) -> bool:
        """Simulate Spark isEmpty function."""
        return self.pandas_df.empty


class MockSparkSession:
    """This class is a mock of SparkSession."""

    ERROR_SQL_NO_SELECT = "Invalid SQL query: Could not find SELECT clause."
    AUTO_FIXTURE = "AUTO_FIXTURE"
    fixture_sequence_index = 0
    # Each SQL statement executed will be appended to this list
    sql_log: list[str] = []

    def __init__(self):
        """Initialize MockSparkSession."""
        self.builder = MagicMock()
        self.tables = {}
        self.df_fixture_path_sequence = []
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.DEBUG)

    def append_fixture_sequence(self, fixture_path: str):
        """Add path to fixture file to use for next SQL execution."""
        self.df_fixture_path_sequence.append(fixture_path)

    def createDataFrame(self, data, schema=None):
        """Emulate Spark createDataFrame using Pandas DataFrame."""
        if isinstance(data, pd.DataFrame):
            return MockDataFrame(data)
        return MockDataFrame(pd.DataFrame(data, columns=schema))

    def registerDataFrameAsTable(self, df, table_name):
        """Register a DataFrame as a table."""
        self.tables[table_name] = df

    def ignore_sql_and_create_df_from_fixture(self, query) -> pd.DataFrame:
        """Use value from fixture_path instance variable rather than SQL."""
        if self.df_fixture_path_sequence:
            json_file_path = os.path.join(
                FIXTURES_DIR,
                self.df_fixture_path_sequence[self.fixture_sequence_index] + ".json",
            )
            self.logger.debug("Query: %s", query)
            self.logger.debug("Loading fixture: %s", json_file_path)
            df = pd.read_json(json_file_path)
            return MockDataFrame(df)
        raise ValueError("No fixture path available in sequence")

    def sql(self, query):
        """Emulation of Spark SQL."""
        if (
            not self.df_fixture_path_sequence
            or self.df_fixture_path_sequence[self.fixture_sequence_index]
            == self.AUTO_FIXTURE
        ):
            self.fixture_sequence_index += 1
            return self.ignore_sql_and_create_df_from_fixture(query)

    def parse_join_clauses(self, statement: sqlparse.sql.Statement) -> list:
        """Parse JOIN clauses and extract join types and conditions."""
        join_matches = []
        from_seen = False
        tokens = [
            token for token in statement.tokens if not token.is_whitespace
        ]
        for i, token in enumerate(tokens):
            if token.ttype == sqlparse.tokens.Keyword:
                token_value = token.value.upper()
                if token_value == "FROM":
                    from_seen = True
                elif from_seen and token_value in (
                    "INNER JOIN",
                    "LEFT JOIN",
                    "RIGHT JOIN",
                    "JOIN",
                ):
                    join_type = token_value
                    next_token = tokens[i + 1] if i + 1 < len(tokens) else None
                    if isinstance(next_token, sqlparse.sql.Identifier):
                        table_name = next_token.get_real_name()
                        on_token = tokens[i + 2] if i + 2 < len(tokens) else None
                        condition_token = (
                            tokens[i + 3] if i + 3 < len(tokens) else None
                        )
                        if (
                            on_token
                            and on_token.ttype is sqlparse.tokens.Keyword
                            and on_token.value.upper() == "ON"
                            and isinstance(condition_token, sqlparse.sql.Comparison)
                        ):
                            left_key = condition_token.left.get_real_name()
                            right_key = condition_token.right.get_real_name()
                            join_matches.append(
                                (join_type, table_name, left_key, right_key)
                            )

        return [
            {
                "join_type": match[0],
                "table_name": match[1],
                "left_key": match[2],
                "right_key": match[3],
            }
            for match in join_matches
        ]

    def load_table_from_json(self, table_name: str) -> pd.DataFrame:
        """Load a JSON file and return a Pandas DataFrame with file data."""
        path_components = table_name.split(".")
        json_file_path = os.path.join(FIXTURES_DIR, *path_components) + ".json"
        if not os.path.exists(json_file_path):
            raise FileNotFoundError(f"Table JSON file not found: {json_file_path}")

        return pd.read_json(json_file_path)

    def _apply_join(
        self, left_df: MockDataFrame, right_df: MockDataFrame, join_args: dict
    ):
        """Perform a join operation between two DataFrames using Pandas merge."""
        join_type = join_args["join_type"]
        left_key = join_args["left_key"]
        right_key = join_args["right_key"]
        how = {
            "INNER JOIN": "inner",
            "JOIN": "inner",
            "LEFT JOIN": "left",
            "RIGHT JOIN": "right",
        }.get(join_type, "inner")
        return pd.merge(
            left_df.toPandas(),
            right_df.toPandas(),
            left_on=left_key,
            right_on=right_key,
            how=how,
        )

    def clean_and_capitalize(self, query: str) -> str:
        """Clean and capitalize SQL keywords in a query."""
        sql_key_words = [
            "select",
            "from",
            "where",
            "and",
            "or",
            "inner",
            "left",
            "right",
            "outer",
            "join",
            "insert",
            "update",
            "delete",
        ]

        def capitalize_match(match):
            word = match.group(0)
            return word.capitalize() if word.lower() in sql_key_words else word

        def replace_spaces_outside_quotes(match):
            quoted_text = match.group(0)
            if quoted_text.startswith("'") or quoted_text.startswith('"'):
                return quoted_text
            return re.sub(r"\s+", " ", quoted_text)

        query = query.strip().replace("\n", " ").replace("\t", " ")
        query = re.sub(r"(['\"].*?['\"]|\s+)", replace_spaces_outside_quotes, query)
        query = re.sub(r"\b\w+\b", capitalize_match, query)
        return query
    def mock_spark_session():
        """Create a Mock spark Session."""
        return MockSparkSession()
