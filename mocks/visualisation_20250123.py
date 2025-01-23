# Dont Delete 
https://docs.databricks.com/en/dashboards/tutorials/create-dashboard.html

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import pandas as pd

# Sample data
data = {
    'Workspace': ['Dev', 'Prod', 'Dev', 'Prod'],
    'TestRun': ['Run1', 'Run2', 'Run1', 'Run2'],
    'Table': ['Table1', 'Table2', 'Table3', 'Table4'],
    'Value': [10, 20, 30, 40]
}
df = pd.DataFrame(data)

# Initialize the Dash app
app = dash.Dash(__name__)

# Layout of the dashboard
app.layout = html.Div([
    html.H1("AB Test Results Dashboard"),
    
    # Dropdown for Workspace
    html.Label("Select Workspace"),
    dcc.Dropdown(
        id='workspace-dropdown',
        options=[{'label': w, 'value': w} for w in df['Workspace'].unique()],
        value=df['Workspace'].unique()[0]
    ),
    
    # Dropdown for Test Run
    html.Label("Select Test Run"),
    dcc.Dropdown(
        id='testrun-dropdown',
        value=df['TestRun'].unique()[0]
    ),
    
    # Dropdown for Table
    html.Label("Select Table"),
    dcc.Dropdown(
        id='table-dropdown',
        value=df['Table'].unique()[0]
    ),
    
    # DataTable to show the results
    dash_table.DataTable(
        id='results-table',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records')
    )
])

# Callback to update Test Run dropdown based on selected Workspace
@app.callback(
    Output('testrun-dropdown', 'options'),
    Input('workspace-dropdown', 'value')
)
def set_testrun_options(selected_workspace):
    filtered_df = df[df['Workspace'] == selected_workspace]
    return [{'label': i, 'value': i} for i in filtered_df['TestRun'].unique()]

# Callback to update Table dropdown based on selected Workspace and Test Run
@app.callback(
    Output('table-dropdown', 'options'),
    [Input('workspace-dropdown', 'value'),
     Input('testrun-dropdown', 'value')]
)
def set_table_options(selected_workspace, selected_testrun):
    filtered_df = df[(df['Workspace'] == selected_workspace) & (df['TestRun'] == selected_testrun)]
    return [{'label': i, 'value': i} for i in filtered_df['Table'].unique()]

# Callback to update DataTable based on selected Workspace, Test Run, and Table
@app.callback(
    Output('results-table', 'data'),
    [Input('workspace-dropdown', 'value'),
     Input('testrun-dropdown', 'value'),
     Input('table-dropdown', 'value')]
)
def update_table(selected_workspace, selected_testrun, selected_table):
    filtered_df = df[(df['Workspace'] == selected_workspace) & 
                     (df['TestRun'] == selected_testrun) & 
                     (df['Table'] == selected_table)]
    return filtered_df.to_dict('records')

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
