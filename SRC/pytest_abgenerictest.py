import pandas as pd

class MockDataFrame:
    def __init__(self, pandas_df=None, columns=None):
        if pandas_df is not None and isinstance(pandas_df, pd.DataFrame):
            self.data = pandas_df
        else:
            self.data = pd.DataFrame(columns=columns)
    
    @property
    def columns(self):
        return self.data.columns.tolist()
    
    @columns.setter
    def columns(self, value):
        self.data.columns = value
    
    def createOrReplaceTempView(self, name):
        """Mock method for createOrReplaceTempView"""
        print(f"Mock createOrReplaceTempView called with name: {name}")

    def count(self):
        """Mock method for count"""
        return len(self.data)

    def collect(self):
        """Mock method for collect"""
        return self.data.values.tolist()

    def withColumnRenamed(self, existing, new):
        """Mock method for withColumnRenamed"""
        new_data = self.data.rename(columns={existing: new})
        return MockDataFrame(new_data)

    def _parse_join_clause(self, on):
        """Mock method for parsing join clause"""
        if isinstance(on, str):
            return on
        elif isinstance(on, list):
            return " AND ".join(on)
        else:
            raise ValueError("Unsupported join clause type")

    def _apply_join(self, other, on, how):
        """Mock method for applying join"""
        join_column = on.split(" = ")[0].split(".")[1]
        joined_data = pd.merge(self.data, other.data, on=join_column, how=how)
        return MockDataFrame(joined_data)

    def join(self, other, on, how="inner"):
        """Mock method for join"""
        return self._apply_join(other, on, how)
    
    def select(self, *cols):
        """Mock method for select"""
        selected_data = self.data[list(cols)]
        return MockDataFrame(selected_data)

    def __getitem__(self, item):
        """Allow column selection using indexing"""
        return self.data[item]

class MockRead:
    def format(self, format_type):
        return self

    def table(self, table_name):
        return MockDataFrame(pd.DataFrame([("value1", "value2")], columns=["key_column1", "key_column2"]))

class MockSparkSession:
    def __init__(self):
        self.read = MockRead()

    def createDataFrame(self, data, schema):
        """Mock method for createDataFrame"""
        return MockDataFrame(pd.DataFrame(data, columns=schema))

    def registerDataFrameAsTable(self, df, name):
        """Mock method for registerDataFrameAsTable"""
        df.createOrReplaceTempView(name)
