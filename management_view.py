from dash import dash_table, Dash, dcc, html, Input, Output, callback
import pandas as pd
import mysql.connector
import datetime
from datetime import date
import plotly.graph_objects as go
import setup

# CALC_FTES_OVER_TIME
def calc_FTEs_over_time(location):
    # Create a data set to plot employees over time for different or all stations
    # If location is none create data set for all stations, otherwise for specific location (station) name
    # Input: location - name of station
    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port, buffered=True)
    mycursor = mydb.cursor()

    today_date = str(datetime.datetime.now().date())

    if (location == "All Locations"):
        location = None

    # Clear previous data
    data['Number of Employees'].clear()
    data['Time'].clear()

    no_FTEs = 0

    if location == None:
        # Get all today's employee entries and sort by the timestamp (latest to earliest)
        mycursor.execute("SELECT * FROM scan_event WHERE JOB_ID IS NULL AND TIMESTAMP LIKE '" + today_date + "%'")
        scan_data = mycursor.fetchall()
        df = pd.DataFrame(scan_data, columns=["ROW_ID", "TIMESTAMP", "STATION", "JOB_ID", "EMPLOYEE_ID", "JOB_STATUS", "EMPLOYEE_ID"])
        df = df.sort_values(by='TIMESTAMP')
        scan_data = df.values.tolist()
    else:
        # Get today's employee entries at a specific location
        mycursor.execute("SELECT * FROM scan_event WHERE STATION=%s AND JOB_ID IS NULL AND TIMESTAMP LIKE '" + today_date + "%'", [(location)])
        scan_data = mycursor.fetchall()

    add_zero_entry = False

    # Find how the number of employees changes over time using the "start" and "stop" employee status variable
    for i in scan_data:
        if (i[6] == "START"):
            no_FTEs += 1
        elif (i[6] == "STOP"):
            no_FTEs -= 1
        if (not add_zero_entry):
            data['Number of Employees'].append(0)
            data['Time'].append(i[1]) 
            add_zero_entry = True  
        data['Number of Employees'].append(no_FTEs)
        data['Time'].append(i[1])
    
    # Append the latest number of employee value with the current time for better data visualization 
    latest_time = datetime.datetime.now()
    # latest_time = main.convert_string_to_datetime("17:00:00 2024-10-04")
    data['Number of Employees'].append(no_FTEs)
    data['Time'].append(latest_time)

    mycursor.close()
    mydb.close()
# CALC_FTES_OVER_TIME 

# Connect to SQL database and create a cursor
mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port, buffered=True)
mycursor = mydb.cursor()

# Execute queries to obtain job and employee data
mycursor.execute("SELECT JOB_ID, START, STOP, DURATION, MAX_NO_FTES, EMPLOYEES_PER_HOUR, TOTAL_FTE_HOURS FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
job_data = mycursor.fetchall()
mycursor.execute("SELECT EMPLOYEE_ID, START, STOP, DURATION FROM " + setup.employee_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
employee_data = mycursor.fetchall()

# Initialize dataframes for job and employee tables in dash app
df1 = pd.DataFrame(job_data, columns=["JOB_ID", "START", "STOP", "DURATION", "MAX_NO_FTES", "EMPLOYEES_PER_HOUR", "TOTAL_FTE_HOURS"])
df2 = pd.DataFrame(employee_data, columns=["EMPLOYEE_ID", "START", "STOP", "DURATION"])

# Close cursor and database connection
mycursor.close()
mydb.close()

############################
############################
##### START OF PROGRAM #####

# Lists for employees over time graph 
data = {
    'Number of Employees': [],
    'Time': [],
}

# Graph layout margin for dash app
layout = go.Layout(
    margin=dict(
        l=10,  # Left margin
        r=10,  # Right margin
        t=20,  # Top margin
        b=10   # Bottom margin
    ),
)

####################
####################
##### DASH APP #####

app = Dash(__name__)
app.layout = html.Div(
    html.Div(children=[
        html.Div(children=[
            html.H1(children="Management Portal", style={'display': 'inline-block', 'padding-left': '10px'}),
            html.H2(id='current-time', style={'display': 'inline-block', 'padding-left': '40px'}),
            html.H2(id = 'current-date', style={'display': 'inline-block', 'padding-left': '30px'}),
        ], style = {"border":"2px #317eab solid",'backgroundColor':'#c2e3f7', 'padding-top': '10px', 'padding-bottom': '10px',}),

        html.Div(children=[
            html.H2(children="Search for a Job or Employee below:", style= {'padding-left': '10px', 'padding-top':'10px'}),
            dcc.Input(id="input1", type="text", placeholder="", style={'padding-left':'10px', 'width': '30%', 'fontSize': '20px'}),
            html.Div(id="search-output", style={'fontSize': '20px', "font-weight": "bold"}),
        ], style={'backgroundColor':'#f3f2f2', 'padding-bottom':'25px'}),

        html.Div(children=[
            dcc.Graph(id='total-employees-graph'),
            dcc.Graph(id='location-employees-graph'),
            dcc.Dropdown(setup.LocationsAll, "All Locations", id='graph-dropdown', multi=True, style={'display': 'inline-block', 'width': '100%', 'fontSize':'20px', 'height': '20%'}),
        ]),

        html.Br(),
        html.Hr(style={'borderWidth': "0.4vh", "width": "100%", "borderColor": "#000000", 'padding-top':'0px'}),
        html.Div(children=[
            html.H2(children="Select a Location to view below:", style={'padding-left': '10px', 'padding-top': '10px', 'padding-bottom': '10px', "width": "28%"}),
            dcc.Dropdown(setup.Locations, setup.Location_name, id='location-dropdown', style={'display': 'inline-block', 'width': '53%', 'fontSize':'20px'}),
            ], style={'backgroundColor':'#f3f2f2', 'padding-bottom':'25px'}),
        html.H1(children={}, id='location-title', style={'padding-left': '10px', "border":"2px #317eab solid",'backgroundColor':'#c2e3f7', 'padding-top': '10px', 'padding-bottom': '10px'}),
        html.H2(children="Active Job:", style={'padding-left': '10px'}),

        dash_table.DataTable(
            id="job_table_data",
            data=df1.to_dict('records'),
            columns=[
                {'id': "JOB_ID", 'name': "Job ID"},
                {'id': "START", 'name': "Start Time"},
                {'id': "STOP", 'name': "Stop Time"},
                {'id': "DURATION", 'name': "Duration"},
                {'id': "MAX_NO_FTES", 'name': "Max. No. FTEs"},
                {'id': "EMPLOYEES_PER_HOUR", 'name': "Employees per hour"},
                {'id': "TOTAL_FTE_HOURS", 'name': "Total FTE hours"},
            ],
            style_cell={'padding': '10px', 'text-align': 'center', 'border': '2px solid black'},
            style_header={'backgroundColor': '#bef9d6','fontWeight': 'bold'},
            ),

        html.Br(),
        html.H2(children="Employees:", style={'padding-left': '10px'}),

        dash_table.DataTable(
            id="employee_table_data",
            data=df2.to_dict('records'),
            columns=[
                {'id': "EMPLOYEE_ID", 'name': "Employee ID"},
                {'id': "START", 'name': "Start Time"},
                {'id': "DURATION", 'name': "Duration"},
                {'id': "STOP", 'name': "Stop Time"},
            ],
            style_cell={'padding': '10px', 'text-align': 'center', 'border': '2px solid black'},
            style_header={'backgroundColor': '#f9c8be','fontWeight': 'bold'},
            ),

        html.Br(),
        dcc.Graph(id='live-FTE-graph'),
        dcc.Interval(
            id='interval-component',
            interval=5*1000, # in milliseconds
            n_intervals=0
        ),
    ]),
)

@app.callback([
          Output('job_table_data', 'data'),
          Output('employee_table_data', 'data'),
          Output('live-FTE-graph', 'figure'),
          Output('current-date', 'children'),
          Output('current-time', 'children'),
          Output('total-employees-graph', 'figure'),
          Output('location-employees-graph', 'figure'),
          Output('location-title', 'children'),
          Output('search-output', 'children')
          ],
          [Input('interval-component', 'n_intervals'),
           Input('location-dropdown', 'value'),
           Input('graph-dropdown', 'value'),
           Input("input1", "value"),
                 ],
          prevent_initial_call=True)
def update_metrics(n_intervals, location_value, graph_dropdown, input1):

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port, buffered=True)
    mycursor = mydb.cursor()

    # Ensure location dropdown options are in list format
    if (isinstance(graph_dropdown, list)):
        locations_graph = graph_dropdown
    else: 
        locations_graph = [graph_dropdown]

    # Default value to set y-axis range
    max_value = 3

    # Plot sum of all employees over time
    calc_FTEs_over_time(None)
    if (data['Number of Employees']):
        max_value = (max(data['Number of Employees'])) + 1
    total_employees_graph = go.Figure(go.Scatter(x = data['Time'], y = data['Number of Employees'], name=setup.LocationsAll[0], mode='lines+markers'))
    total_employees_graph.update_layout(margin=dict(t=40, b=10),yaxis_range=[0,max_value], title_text="<b>"+"Total Number of Employees over time"+"<b>", title_x=0.5, title_y = 0.95, xaxis_title="Time (hours)", yaxis_title="No. of Employees")
    total_employees_graph.add_scatter(x=[data['Time'][-1]], y = [data['Number of Employees'][-1]], mode = 'markers+text', text = data['Number of Employees'][-1],textposition='top right', showlegend=False, line_color='rgb(0, 0, 0)')

    # Plot employees over time for each station and all locations on same figure
    max_value = 0
    location_employees_graph = go.Figure()
    for i in locations_graph:
        calc_FTEs_over_time(i)
        location_employees_graph.add_trace(go.Scatter(x = data['Time'], y = data['Number of Employees'], name=i, mode='lines+markers'))
        location_employees_graph.add_scatter(x=[data['Time'][-1]], y = [data['Number of Employees'][-1]], mode = 'markers+text', text = data['Number of Employees'][-1],textposition='top right', showlegend=False, line_color='rgb(0, 0, 0)')
        if (data['Number of Employees']):
            if ((max(data['Number of Employees']) + 1) > max_value):
                max_value = max(data['Number of Employees']) + 1
    location_employees_graph.update_layout(margin=dict(t=40, b=10),yaxis_range=[0,max_value], title_text="<b>"+"Total Number of Employees at each Station over time"+"<b>", title_x=0.5, title_y = 0.95, xaxis_title="Time (hours)", yaxis_title="No. of Employees")

    # Recreate location view of a selected station

    # Job data for a specific location
    mycursor.execute("SELECT JOB_ID, START, STOP, DURATION, MAX_NO_FTES, EMPLOYEES_PER_HOUR, TOTAL_FTE_HOURS FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([location_value]))
    job_data = mycursor.fetchall()
    df1 = pd.DataFrame(job_data, columns=["JOB_ID", "START", "STOP", "DURATION", "MAX_NO_FTES", "EMPLOYEES_PER_HOUR", "TOTAL_FTE_HOURS"])

    # Employee data for a specific location
    mycursor.execute("SELECT EMPLOYEE_ID, START, STOP, DURATION FROM " + setup.employee_table + " WHERE STOP IS NULL AND STATION=%s", ([location_value]))
    employee_data = mycursor.fetchall()
    df2 = pd.DataFrame(employee_data, columns=["EMPLOYEE_ID", "START", "STOP", "DURATION"])
    
    # Get the current time and date without milliseconds
    current_time = datetime.datetime.now()
    current_date = str(datetime.datetime.now().date())
    current_time = (current_time.strftime("%Y-%m-%d %H:%M:%S.%f")[0:19])
    
    # Create employees over time figure for the specific station
    calc_FTEs_over_time(location_value)
    fig = go.Figure(go.Scatter(x = data['Time'], y = data['Number of Employees'], name=setup.Location_name, mode='lines+markers'))
    max_value = max(data['Number of Employees']) + 1
    fig.update_layout(margin=dict(t=40, b=10),yaxis_range=[0,max_value], title_text=("<b>"+"Total Number of Employees over time at " + location_value + "<b>"), title_x=0.5, title_y = 0.95, xaxis_title="Time (hours)", yaxis_title="No. of Employees")
    fig.add_scatter(x=[data['Time'][-1]], y = [data['Number of Employees'][-1]], mode = 'markers+text', text = data['Number of Employees'][-1],textposition='top right', showlegend=False, line_color='rgb(0, 0, 0)')

    # Find and show results for the employee and job search feature
    if (not input1):
        output = ""
    elif (input1[0] == 'E'):
        mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE EMPLOYEE_ID=%s AND STOP IS NULL", ([input1]))
        output = mycursor.fetchall()
        if (not output):
            mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE EMPLOYEE_ID=%s AND START LIKE '%" + current_date + "'", ([input1]))
            output = mycursor.fetchall()
            if (not output):
                output = html.Div("Employee not found.", style={'padding-top': '20px', 'padding-left': '40px'})
            else:
                output = output[-1]
                output = html.Div([
                html.Div("Employee " + input1[1:], style={'padding-top': '20px', 'padding-left': '40px'}),
                html.Div("Last Location: " + output[1], style={'padding-top': '10px', 'padding-left': '40px'}), 
                html.Div("Stop time: " + output[3], style={'padding-top': '10px', 'padding-left': '40px'}),
                ])
        else:
            output = output[0]
            output = html.Div([
                html.Div("Employee " + input1[1:], style={'padding-top': '20px', 'padding-left': '40px'}),
                html.Div("Current Location: " + output[1], style={'padding-top': '10px', 'padding-left': '40px'}), 
                html.Div("Start time: " + output[2], style={'padding-top': '10px', 'padding-left': '40px'}),
                ])
    else:
        mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE JOB_ID=%s AND STOP IS NULL", ([input1]))
        output = mycursor.fetchall()
        if (not output):
            mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE JOB_ID=%s AND START LIKE '%" + current_date + "'", ([input1]))
            output = mycursor.fetchall()
            if (not output):
                output = html.Div("Job not found.", style={'padding-top': '20px', 'padding-left': '40px'})
            else:
                output = output[-1]
                output = html.Div([
                html.Div("Job " + input1, style={'padding-top': '20px', 'padding-left': '40px'}),
                html.Div("Last Location: " + output[1], style={'padding-top': '10px', 'padding-left': '40px'}), 
                html.Div("Stop time: " + output[3], style={'padding-top': '10px', 'padding-left': '40px'}),
                ])
        else:
            output = output[0]
            output = html.Div([
                html.Div("Job " + input1, style={'padding-top': '20px', 'padding-left': '40px'}),
                html.Div("Current Location: " + output[1], style={'padding-top': '10px', 'padding-left': '40px'}), 
                html.Div("Start time: " + output[2], style={'padding-top': '10px','padding-left': '40px'}),
                ])
    
    mycursor.close()
    mydb.close()

    current_date = date.today()
    current_time = str(datetime.datetime.now().time())[0:8]

    return (df1.to_dict('records'), df2.to_dict('records'), fig, current_date, current_time, total_employees_graph, location_employees_graph, location_value, output)

if __name__ == '__main__':
    app.run_server(debug=True, use_reloader=False)
### DASH APP ###
