# Dont Delete 
https://docs.databricks.com/en/dashboards/tutorials/create-dashboard.html

------------


-- Create a new view or table with the desired output columns
CREATE OR REPLACE VIEW en_poc_dev.sparta_work.sparta_ab_audit_result_transformed AS
SELECT
    CASE
        WHEN test_name LIKE '%dev%' THEN 'Dev'
        WHEN test_name LIKE '%test%' THEN 'Test'
        WHEN test_name LIKE '%prod%' THEN 'Prod'
        ELSE 'Unknown'
    END AS Workspace,
    Table_a AS Base_Table,
    Table_b AS Test_Table,
    schema_mismatch AS schema_mismatch,
    mismatch_count AS actual_mismatch,
    CASE
        WHEN mismatch_count = 0 THEN 'No'
        ELSE 'Yes'
    END AS Table_mismatch
FROM
    en_poc_dev.sparta_work.sparta_ab_audit_result;
