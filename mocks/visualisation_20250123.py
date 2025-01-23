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

--------------------------------------


import subprocess
import pandas as pd
# Other necessary imports...

def get_git_branch_name():
    try:
        result = subprocess.run(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            print("Error retrieving Git branch name:", result.stderr)
            return "unknown"
    except Exception as e:
        print("Exception occurred while retrieving Git branch name:", str(e))
        return "unknown"

def update_audit_table(audit_table_name, data):
    # Assuming 'data' is a DataFrame containing the audit data to be inserted
    # Retrieve the Git branch name
    branch_name = get_git_branch_name()
    
    # Add the branch name as a new column to the DataFrame
    data['branch_name'] = branch_name
    
    # Example: Save the updated DataFrame to the audit table
    # This will depend on how you are interacting with your database
    # For example, using SQLAlchemy:
    from sqlalchemy import create_engine
    engine = create_engine('your_database_connection_string')
    
    try:
        data.to_sql(audit_table_name, engine, if_exists='append', index=False)
        print(f"Data successfully inserted into {audit_table_name}")
    except Exception as e:
        print(f"Error inserting data into {audit_table_name}: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Sample audit data
    audit_data = {
        'column1': ['value1', 'value2'],
        'column2': ['value3', 'value4']
    }
    audit_df = pd.DataFrame(audit_data)
    
    # Update the audit table with the sample data
    update_audit_table('audit_table_name', audit_df)
