test_name: The name of the A/B test.
table_a: The name of the first Delta table (A variant).
table_b: The name of the second Delta table (B variant).
column_name: The name of the column being validated.
schema_mismatch: A flag indicating if there is a schema mismatch (boolean).
data_mismatch: A flag indicating if there is a data mismatch (boolean).
mismatch_count: The count of mismatched rows for the column.
validation_errors: Details of validation errors (string).
