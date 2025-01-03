-- Create a view to simplify aggregation
CREATE OR REPLACE TEMP VIEW unmatched_counts AS
SELECT
    'clm_id' AS column_name,
    COUNT(*) AS unmatched_count
FROM result_table
WHERE clm_id_result = 'unmatch'
UNION ALL
SELECT
    'cnt_name' AS column_name,
    COUNT(*) AS unmatched_count
FROM result_table
WHERE cnt_name_result = 'unmatch'
UNION ALL
SELECT
    'clime_id' AS column_name,
    COUNT(*) AS unmatched_count
FROM result_table
WHERE clime_id_result = 'unmatch';

-- Select from the view to display the unmatched counts for each column
SELECT *
FROM unmatched_counts
ORDER BY unmatched_count DESC;
