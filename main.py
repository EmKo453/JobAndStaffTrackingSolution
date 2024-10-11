from dash import dash_table, Dash, dcc, html, Input, Output, callback
import pandas as pd
import paho.mqtt.client as mqtt
import mysql.connector
import time, datetime
from datetime import date, timedelta
import threading
import plotly.graph_objects as go
import plotly_express as px
import setup

# READ_SCANNER
def read_scanner():
    # Subscribe and listen to a topic of the MQTT broker for messages
    # Call on_message when a message has been received

    while True:
        client.loop_start()

        # Subscribe to topic
        client.subscribe("+/feeds/jobs")

        # Function call when message received
        client.on_message= on_message

        time.sleep(1)

        client.loop_stop()
# READ_SCANNER

# ON_MESSAGE
def on_message(client, userdata, message):
    # Extracts the values from a message received from the MQTT broker
    # Adds employee/job scan to the database
    # Inputs: message - string message received from MQTT broker

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    # Print message
    print("Received message: ", str(message.payload.decode("utf‐8")))
    
    # Decode message
    m = message.payload.decode("utf-8")
    substrings = m.split(", ")
    id = substrings[0][12:len(substrings[0]) -1]
    location = substrings[1][13:len(substrings[1]) -1]

    # Format timestamp in datetime format
    m_timestamp = substrings[2][14:len(substrings[2]) -2]
    m_timestamp = m_timestamp.split("T")
    m_timestamp = m_timestamp[0] + " " + m_timestamp[1][0:14]

    # Convert datetime timestamp to string
    timestamp = m_timestamp[11:19] + (" ") + m_timestamp[0:10]
    
    m_timestamp = convert_string_to_datetime(timestamp)

    if (id[0] == 'E'):
        # Add employee to database
        add_employee_scan_to_db_tables(id, timestamp, m_timestamp, location)
    else:
        # Add job to database
        add_job_scan_to_db_tables(id, timestamp, m_timestamp, location)

    mycursor.close()
    mydb.close()
# ON_MESSAGE

# ADD_JOB_SCAN_TO_DB_TABLES
def add_job_scan_to_db_tables(id, timestamp, datetimestamp, location):
    # Opens a job by creating a new database entry if new job is starting
    # Adds a new job entry to the jobs table and the scan event tables by adding a "start" entry
    # "Signs in" employees into jobs by adding "in" entries to the scan event table
    # "Signs out" employees from jobs by adding "out" entries to the scan event table
    # Closes a job by updating the stop time of the jobs table and adding "stop" entries to the scan event table
    # Inputs: id - job id, timestamp - string timestamp, datetimestamp - datetime timestamp, location - station name

    if (is_duplicate(id, location)):
        
        start_time = get_start_time_of_open_entry(id, location)
        
        mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
        mycursor = mydb.cursor()
        
        # Stop job at the station
        mycursor.execute("UPDATE " + setup.job_table + " SET STOP=%s WHERE JOB_ID=%s AND STOP IS NULL AND STATION=%s", (timestamp, id, location))
        
        mydb.commit()
        mycursor.close()
        mydb.close()
        
        update_duration(id, start_time, timestamp, timestamp, 0)
        
        mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
        mycursor = mydb.cursor()
        
        values = (datetimestamp, location, id, None, None, "FINISH")
        mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", values)

        # Sign out all employees assigned to the job
        mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", ([location]))
        employees = mycursor.fetchall()
        for i in employees:
            values = (datetimestamp, location, id, i[0], None, "OUT")
            mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", values)
        mydb.commit()
        calculate_total_FTE_hours(id, location, start_time, timestamp, datetimestamp)
    else:
        
        mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
        mycursor = mydb.cursor()
        
        # Start job at the station
        values = (id, location, timestamp, None, ("0:00:00"), 0, 0.0, ("0:00:00"), None, 0)
        mycursor.execute("INSERT INTO " + setup.job_table + " (JOB_ID, STATION, START, STOP, DURATION, MAX_NO_FTES, EMPLOYEES_PER_HOUR, TOTAL_FTE_HOURS, WARNINGS, BREAK_TIME) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", values)

        values = (datetimestamp, location, id, None, None, "BEGIN")
        mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", values)

        # Sign in all open employees into the job
        mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", ([location]))
        employees = mycursor.fetchall()
        for i in employees:
            values = (datetimestamp, location, id, i[0], None, "IN")
            mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", values)
        mydb.commit()
        mycursor.close()
        mydb.close()

        increment_maximum_FTEs(id)
# ADD_JOB_SCAN_TO_DB_TABLES  

# ADD_EMPLOYEE_SCAN_TO_DB_TABLES
def add_employee_scan_to_db_tables(id, timestamp, datetimestamp, location):
    # Opens an employee by creating a new database entry if new employee is starting
    # Adds a new employee entry to the employees table and the scan event tables by adding a "start" entry
    # "Signs in" employee into open job by adding "in" entries to the scan event table
    # "Signs out" employee from open job by adding "out" entries to the scan event table
    # Closes an employee by updating the stop time of the employees table and adding "stop" entries to the scan event table
    # Inputs: id - employee id, timestamp - string timestamp, datetimestamp - datetime timestamp, location - station name

    if (is_duplicate(id, location)):
        # Stop employee at the station

        start_time = get_start_time_of_open_entry(id, location)
        update_duration(id, start_time, timestamp, timestamp, 0)

        mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
        mycursor = mydb.cursor()
        mycursor.execute("UPDATE " + setup.employee_table + " SET STOP=%s WHERE EMPLOYEE_ID=%s AND STOP IS NULL AND STATION=%s", (timestamp, id, location))

        mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([location]))
        jobs = mycursor.fetchall()
        
        # Sign out the employee if assigned to a job
        for i in jobs:
            values = (datetimestamp, location, i[0], id, None, "OUT")
            mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", values)

        # Stop employee at the station
        values = (datetimestamp, location, None, id, None, "STOP")
        mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", values)
        
        mydb.commit()
        mycursor.close()
        mydb.close() 
    else:
        mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
        mycursor = mydb.cursor()
        
        # Start employee at the station
        values = (id, location, timestamp, None, ("0:00:00"), None, 0)
        mycursor.execute("INSERT INTO " + setup.employee_table + " (EMPLOYEE_ID, STATION, START, STOP, DURATION, WARNINGS, BREAK_TIME) VALUES (%s, %s, %s, %s, %s, %s, %s)", values)
        values = (datetimestamp, location, None, id, None, "START")
        mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", values)
        # Sign employee into the open job
        mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([location]))
        jobs = mycursor.fetchall()
        for i in jobs:
            values = (datetimestamp, location, i[0], id, None, "IN")
            mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", values)
        
        mydb.commit()
        mycursor.close()
        mydb.close()    
        
        increment_maximum_FTEs(id)
# ADD_EMPLOYEE_SCAN_TO_DB_TABLES  

# CHECK_FOR_TABLE_DUPLICATE
def is_duplicate(id, location):
    # Checks if job/employee is already open at a location
    # Returns true if job/employee is open at a location, false if not
    # Inputs: id - job or employee ID, location - station name

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    # Check if an open job or employee is already at the station
    if (id[0] == 'E'):
        mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE EMPLOYEE_ID=%s AND STATION=%s AND STOP IS NULL", (id, location))
        data = mycursor.fetchall()
    else:
        mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE JOB_ID=%s AND STATION=%s AND STOP IS NULL", (id, location))
        data = mycursor.fetchall()

    mycursor.close()
    mydb.close()

    # Check if data list is empty
    if (not data):
        # List is empty, no duplicate exists
        return False
    else:
        # List is not empty, job/employee already exists
        return True
# CHECK_FOR_TABLE_DUPLICATE

# SET_DURATION_OF_ALL_DATA
def set_duration_of_all_data(current_time):
    # Loops through all open jobs and employees at the station and updates their duration
    # Inputs: current_time - current time in datetime format

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    # Get all open jobs at the station
    mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
    data = mycursor.fetchall()
    # Update the duration of all open jobs
    for i in data:
        update_duration(i[0],i[2], None, current_time, i[9])
    
    # Get all open employees
    mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", ([setup.Location_name]))
    data = mycursor.fetchall()
    # Update the duration of all open employees
    for i in data:
        update_duration(i[0],i[2], None, current_time, i[6])

    mycursor.close()
    mydb.close()
# SET_DURATION_OF_ALL_DATA

# UPDATE_DURATION
def update_duration(id, start, stop, current_time, break_time):
    # Updates the duration of job/employee entry
    # If stop input is none job/employee is still open, current time is used to calculate the duration
    # If stop input is not none job/employee is being closed and the final total duration is calculated
    # Inputs: id - job/employee id, start - start time string, stop - stop time string, current_time - current_time in datetime format,
    # break_time - integer of break time minutes

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    break_time = datetime.timedelta(hours=0, minutes=break_time, seconds=0)

    if (stop == None):
        # If stop time not given set it to the current time
        start_time = convert_string_to_datetime(start)

        # Calculate the duration
        duration = current_time - start_time - break_time
        duration = str(duration).split(".")[0]

        if (id[0] == 'E'):
            mycursor.execute("UPDATE " + setup.employee_table + " SET DURATION=%s WHERE EMPLOYEE_ID=%s AND START=%s", (duration, id, start))
        else:
            mycursor.execute("UPDATE " + setup.job_table + " SET DURATION=%s WHERE JOB_ID=%s AND START=%s", (duration, id, start))
    elif(stop != None):
        end_time = convert_string_to_datetime(stop)
        start_time = convert_string_to_datetime(start)
        duration = end_time - start_time - break_time

        if (id[0] == 'E'):
            mycursor.execute("UPDATE " + setup.employee_table + " SET DURATION=%s WHERE EMPLOYEE_ID=%s AND START=%s", (duration, id, start))
        else:
            mycursor.execute("UPDATE " + setup.job_table + " SET DURATION=%s WHERE JOB_ID=%s AND START=%s", (duration, id, start))

    mydb.commit()
    mycursor.close()
    mydb.close()
# UPDATE_DURATION

# CONVERT_STRING_TO_DATETIME
def convert_string_to_datetime(i):
    # Converts from string type to datetime type
    # Inputs: i - timestamp in string type

    if (i != None):
        datetime_timestamp = datetime.datetime.combine((datetime.datetime.strptime(i[9:19], '%Y-%m-%d')),(datetime.time(int(i[0:2]), int(i[3:5]), int(i[6:8]))))
        return datetime_timestamp
    return None
# CONVERT_STRING_TO_DATETIME

# GET_START_TIME_OF_OPEN_ENTRY
def get_start_time_of_open_entry(id, location):
    # Searches for and returns the start time of an open entry
    # Returns none if employee/job could not be found
    # Inputs: id - job/employee id, location - station name
    # Outputs: start time (string) of active job/employee at a station

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    if (id[0] == 'E'):
        mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE EMPLOYEE_ID=%s AND STATION=%s AND STOP IS NULL", (id, location))
        data = mycursor.fetchall()
    else:
        mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE JOB_ID=%s AND STATION=%s AND STOP IS NULL", (id, location))
        data = mycursor.fetchall()

    if (not data):
        return None
    else:
        return data[0][2]
# GET_START_TIME_OF_OPEN_ENTRY 

# INCREMENT_MAXIMUM_FTES
def increment_maximum_FTEs(id):
    # Finds the current maximum number of employees and updates it if greater than previous value
    # Inputs: id - job or employee id

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    if (id[0] == 'E'):
        # Obtain open job
        mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
        job = mycursor.fetchall()

        # Obtain all open employees at the station
        mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", ([setup.Location_name]))
        employees = mycursor.fetchall()
        max_FTE_number = 0

        # Calculate the current max FTE number
        for i in employees:
            max_FTE_number += 1
        max_FTE_number += 1

        if (not job):
            return
        else:
            # Convert from list to tuple
            job = job[0]

        # If current max FTE number exceeds previous, update the value
        if (max_FTE_number > job[5]):
            mycursor.execute("UPDATE " + setup.job_table + " SET MAX_NO_FTES=%s WHERE JOB_ID=%s AND STATION=%s AND STOP IS NULL", (max_FTE_number, job[0],setup.Location_name))
    else:
        # Obtain all open employees
        mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", ([setup.Location_name]))
        employees = mycursor.fetchall()
        max_FTE_number = 0

        # Calculate the current max FTE number
        for i in employees:
            max_FTE_number += 1
        
        mycursor.execute("UPDATE " + setup.job_table + " SET MAX_NO_FTES=%s WHERE JOB_ID=%s AND STATION=%s AND STOP IS NULL", (max_FTE_number, id, setup.Location_name))

    mydb.commit()
    mycursor.close()
    mydb.close()
# INCREMENT_MAXIMUM_FTES

# CALCULATE_TOTAL_FTE_HOURS
def calculate_total_FTE_hours(id, location, start, stop, current_time):
    # Calculates the total amount of time employees spent working on a job
    # Inputs: id - job/employee id, location - station name, start - string start time, stop - string stop time,
    # current_time - datetime current time

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()
    job_start_time = convert_string_to_datetime(start)

    total_FTE_hours = datetime.timedelta(hours=0, minutes=0, seconds=0)
    
    #if (job_start_time.date() != current_time.date()):
    # Return if job did not start today
    #    return

    if (stop == None):
        job_end_time = current_time
    else:
        job_end_time = convert_string_to_datetime(stop)

    today_date = str(date.today())
    mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE START LIKE '%" + today_date + "' AND STATION=%s", ([setup.Location_name]))
    employees = mycursor.fetchall()
    for i in employees:
        employee_start_time = convert_string_to_datetime(i[2])
        if (i[3] == None):
            # Employee is still open, set end time to current time
            employee_end_time = current_time
        else:
            # Set end time to employee stop time
            employee_end_time = convert_string_to_datetime(i[3])
        start_time = None
        end_time = None

        # Determine start time
        if (job_start_time >= employee_start_time and employee_end_time > job_start_time):
        # Job starts after employee starts and employee ends after job start
            start_time = job_start_time
        elif (job_start_time < employee_start_time):
            # Employee starts after job starts
            start_time = employee_start_time

        # Determine end time
        if (employee_end_time >= job_end_time):
            # Employee ends after job ends
            end_time = job_end_time
        elif (employee_end_time < job_end_time):
            # Employee ends before job ends
            end_time = employee_end_time

        break_amount = 0
        if (start_time != None and end_time != None):
            for k in range(len(breaks)):
                breakT = datetime.time(int(breaks[k][0][0:2]),int(breaks[k][0][3:5]),int(breaks[k][0][6:8]))
                if (breakT >= start_time.time() and breakT <= end_time.time()):
                    break_amount = break_amount + int(breaks[k][1])
            break_time = datetime.timedelta(hours=0, minutes=break_amount, seconds=0)
            total_FTE_hours = total_FTE_hours + (end_time - start_time - break_time) 
    if (total_FTE_hours >= datetime.timedelta(hours=23, minutes=59, seconds=59)):
        total_FTE_hours =  str(total_FTE_hours)
    else:            
        total_FTE_hours = str(total_FTE_hours)[0:8]
    mycursor.execute("UPDATE " + setup.job_table + " SET TOTAL_FTE_HOURS=%s WHERE JOB_ID=%s AND STATION=%s AND STOP IS NULL", (total_FTE_hours, id, location))

    mydb.commit()
    mycursor.close()
    mydb.close()
# CALCULATE_TOTAL_FTE_HOURS

# TOTAL_LOCATION_FTES_OVER_TIME
def total_location_FTEs_over_time():
    # Creates a dataset for the number of employees over time
    # Outputs: data - dictionary type composed of lists of number of employees and corresponding time values

    # Empty lists for employees over time graph 
    data = {
        'Number of Employees': [],
        'Time': [],
        }

    # Connect to SQL database and create a cursor for queries
    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    no_FTEs = 0

    current_date = str(datetime.datetime.now().date())

    # Select all entries from today
    mycursor.execute("SELECT * FROM " + setup.scan_event + " WHERE STATION=%s AND JOB_ID IS NULL AND TIMESTAMP LIKE '%" + current_date + "%'", ([setup.Location_name]))
    scan_data = mycursor.fetchall()

    # Increment/decrement number of FTEs
    for i in scan_data:
        if (i[6] == "START"):
            no_FTEs += 1
        elif (i[6] == "STOP"):
            no_FTEs -= 1
        data['Number of Employees'].append(no_FTEs)
        data['Time'].append(i[1])
    
    # Add last entry in data to be current time and current number of FTEs for better data visualization
    current_time = datetime.datetime.now()

    data["Number of Employees"].append(no_FTEs)
    data["Time"].append(current_time)

    mycursor.close()
    mydb.close()

    return data
# TOTAL_LOCATION_FTES_OVER_TIME 

# CALCULATE_NO_OF_EMPLOYEES_PER_HOUR
def calculate_no_of_employees_per_hour():
    # Calculates the number of employees per hour for a job
    # Example: 4 total employees worked on a job for 2 hours, employees per hour would be 2

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    current_time = datetime.datetime.now()
    today_date = str(current_time.date())

    # Get all open jobs
    mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
    job = mycursor.fetchall()

    # Get all open and closed employees that worked today
    mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND START LIKE '%" + today_date + "'", ([setup.Location_name]))
    employees = mycursor.fetchall()

    for i in job:
        no_of_employees = 0
        start_time = convert_string_to_datetime(i[2])
        if (i[3] == None):
            stop_time = current_time
        else:
            stop_time = convert_string_to_datetime(i[3])

        # Find amount of hours job has been open
        job_duration_hour = (int(str((stop_time - start_time))[0:1]))
        if (job_duration_hour < 1):
            job_duration_hour = 1

        # Find the amount of employees that were open within hourly increments
        count = 0
        while (count < job_duration_hour):
            hour_end = start_time + timedelta(hours=1)
            for j in employees:
                employee_start = convert_string_to_datetime(j[2])
                if (j[3] == None):
                    employee_end = current_time
                else:
                    employee_end = convert_string_to_datetime(j[3])
                if (employee_end >= start_time and employee_start <= hour_end):
                    no_of_employees += 1
            start_time = start_time + timedelta(hours=1)
            count += 1
        employees_per_hour = str(no_of_employees / job_duration_hour)[0:4]  
        mycursor.execute("UPDATE " + setup.job_table + " SET EMPLOYEES_PER_HOUR=%s WHERE JOB_ID=%s AND STATION=%s AND START=%s AND STOP IS NULL", (employees_per_hour, i[0], i[1], i[2]))

    mydb.commit()
    mycursor.close()
    mydb.close()
# CALCULATE_NO_OF_EMPLOYEES_PER_HOUR 

# CHECK_FOR_WARNINGS
def check_for_warnings():
    check_for_multiple_open_jobs_or_employees()
# CHECK_FOR_WARNINGS

# CHECK_FOR_MULTIPLE_OPEN_JOBS_OR_EMPLOYEES
def check_for_multiple_open_jobs_or_employees():
    # Checks whether a job or employee is open at multiple stations at once
    # Adds a warning message if job/employee is open at multiple stations at once

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    # Obtain all open jobs at the station
    mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
    jobs = mycursor.fetchall()

    # Obtain all open employees at the station
    mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", ([setup.Location_name]))
    employees = mycursor.fetchall()

    # Obtain all open job IDs at all other stations
    mycursor.execute("SELECT JOB_ID FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION!=%s", ([setup.Location_name]))
    job_df = mycursor.fetchall()
    job_df = pd.DataFrame(job_df, columns=["JOB_ID"])

    # Obtain all open employee IDs at all other stations
    mycursor.execute("SELECT EMPLOYEE_ID FROM " + setup.employee_table + " WHERE STOP IS NULL AND STATION!=%s", ([setup.Location_name]))
    employee_df = mycursor.fetchall()
    employee_df = pd.DataFrame(employee_df, columns=["EMPLOYEE_ID"])

    warning = None

    # If job is open at multiple locations add warning message
    for i in jobs:
        if i[0] in job_df.values:
            if (i[8] == None):
                warning = "MULTIPLE OPEN JOBs"
                mycursor.execute("UPDATE " + setup.job_table + " SET WARNINGS=%s WHERE JOB_ID=%s AND STATION=%s AND STOP IS NULL", (warning, i[0], setup.Location_name))
            elif (i[5].find("MULTIPLE OPEN JOBS")):
                warning = i[8] + ", MULTIPLE OPEN JOBS"
                mycursor.execute("UPDATE " + setup.job_table + " SET WARNINGS=%s WHERE JOB_ID=%s AND STATION=%s AND STOP IS NULL", (warning, i[0], setup.Location_name))

    # If employee is open at multiple locations add warning message
    for j in employees:
        if (j[0] in employee_df.values):
            if (j[5] == None):
                warning = "MULTIPLE OPEN EMPLOYEES"
                mycursor.execute("UPDATE " + setup.employee_table + " SET WARNINGS=%s WHERE EMPLOYEE_ID=%s AND STATION=%s AND STOP IS NULL", (warning, j[0], setup.Location_name))
            elif (not j[5].find("MULTIPLE OPEN EMPLOYEES")):
                warning = j[5] + ", MULTIPLE OPEN EMPLOYEES"
                mycursor.execute("UPDATE " + setup.employee_table + " SET WARNINGS=%s WHERE EMPLOYEE_ID=%s AND STATION=%s AND STOP IS NULL", (warning, j[0], setup.Location_name))
    mydb.commit()
    mycursor.close()
    mydb.close()
# CHECK_FOR_MULTIPLE_OPEN_JOBS_OR_EMPLOYEES

# CLOSE_OLD_JOBS_AND_EMPLOYEES
def close_old_jobs_and_employees():
    # Closed jobs and employees that started the previous day and weren't closed
    # Sets the stop time to a default value of 23:59:59

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    current_time = datetime.datetime.now()

    end_time = str(datetime.time(hour=23, minute=59, second=59))

    # Obtain all open jobs at the station
    mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
    jobs = mycursor.fetchall()

    # If start date does not equal current date, close job and add warning message
    for i in jobs:
        start = convert_string_to_datetime(i[2])
        if (start.date() != current_time.date()):
            end_datetime = end_time + " " + str(start.date())
            timestamp = convert_string_to_datetime(end_datetime)
            if (i[8] == None):
                warning = "JOB WAS NOT CLOSED"
            else:
                warning = i[8] + ", " + "JOB WAS NOT CLOSED"
            mycursor.execute("UPDATE jobs SET STOP=%s, WARNINGS=%s WHERE JOB_ID=%s AND STATION=%s AND STOP IS NULL", (end_datetime, warning, i[0], setup.Location_name))
            mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", (timestamp, setup.Location_name, i[0], None, "OUT", None))

            mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", [setup.Location_name])
            job_employees = mycursor.fetchall()
            for j in job_employees:
                mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", (timestamp, setup.Location_name, i[0], j[0], None, "OUT"))

    # Obtain all open employees at the station
    mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", ([setup.Location_name]))
    employees = mycursor.fetchall()

    # If start date does not equal current date, close employee and add warning message
    for i in employees:
        start = convert_string_to_datetime(i[2])
        if (start.date() != current_time.date()):
            end_datetime = end_time + " " + str(start.date())
            timestamp = convert_string_to_datetime(end_datetime)
            if (i[5] == None):
                warning = "EMPLOYEE WAS NOT CLOSED"
            else:
                warning = i[5] + ", " + "EMPLOYEE WAS NOT CLOSED"
            mycursor.execute("UPDATE " + setup.employee_table + " SET STOP=%s, WARNINGS=%s WHERE EMPLOYEE_ID=%s AND STATION=%s AND STOP IS NULL", (end_datetime, warning, i[0], setup.Location_name))
            mycursor.execute("INSERT INTO " + setup.scan_event + " (TIMESTAMP, STATION, JOB_ID, EMPLOYEE_ID, JOB_STATUS, EMPLOYEE_STATUS) VALUES (%s, %s, %s, %s, %s, %s)", (timestamp, setup.Location_name, None, i[0], None, "STOP"))

    mydb.commit()
    mycursor.close()
    mydb.close()
# CLOSE_OLD_JOBS_AND_EMPLOYEES

# CHECK_FOR_BREAKS
def check_for_breaks():
    # Checks whether a break has occurred for a job/employee
    # Updates the break time of a job/employee if a break has occurred

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    current_time = datetime.datetime.now().time()

    mycursor.execute("SELECT * FROM " + setup.employee_table + " WHERE STATION=%s AND STOP IS NULL", ([setup.Location_name]))
    employees = mycursor.fetchall()

    for i in employees:
        breakTime = 0
        start = convert_string_to_datetime(i[2])
        for j in range(len(breaks)):
            # Convert break value from string to time type
            breakT = datetime.time(int(breaks[j][0][0:2]),int(breaks[j][0][3:5]),int(breaks[j][0][6:8]))
            # If break occurred while job open, increment break value
            if (breakT >= start.time() and breakT <= current_time):
                breakTime = breakTime + int(breaks[j][1])
        if (i[6] != breakTime):
            mycursor.execute("UPDATE " + setup.employee_table + " SET BREAK_TIME=%s WHERE EMPLOYEE_ID=%s",(breakTime, i[0]))

    mycursor.execute("SELECT * FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
    jobs = mycursor.fetchall()

    for i in jobs:
        breakTime = 0
        start = convert_string_to_datetime(i[2])
        for j in range(len(breaks)):
            # Convert break value from string to time type
            breakT = datetime.time(int(breaks[j][0][0:2]),int(breaks[j][0][3:5]),int(breaks[j][0][6:8]))
            # If break occurred while employee open, increment break value
            if (breakT >= start.time() and breakT <= current_time):
                breakTime = breakTime + int(breaks[j][1])
        if (i[9] != breakTime):
            mycursor.execute("UPDATE " + setup.job_table + " SET BREAK_TIME=%s WHERE JOB_ID=%s",(breakTime, i[0]))

    mydb.commit()
    mycursor.close()
    mydb.close()
# CHECK_FOR_BREAKS  

############################
##### START OF PROGRAM #####

# Close open jobs and employees that did not start today 
close_old_jobs_and_employees()

# Connect to mqtt broker
client = mqtt.Client()
client.connect(setup.mqttBroker,setup.mqtt_port,90)

# Create thread to receive barcode scans
threading.Thread(target = read_scanner, daemon = True).start()

# read break times from text file
breaks = []
file = open("breaks.txt", "r").readlines()
for lines in file:
    breaks.append([lines[0:8], lines[10:12]])

# Graph layout margin for dash app
layout = go.Layout(
    margin=dict(
        l=10,  # Left margin
        r=10,  # Right margin
        t=20,  # Top margin
        b=10   # Bottom margin
    ),
)

# Connect to SQL database and create a cursor
mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
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

####################
##### DASH APP #####

app = Dash(__name__)
app.layout = html.Div(
    html.Div(children=[
        html.Div(children=[
            html.H1(children=setup.Location_name, style={'display': 'inline-block', 'padding-left': '10px',}),
            html.H2(id='current-time', style={'display': 'inline-block', 'padding-left': '40px'}),
            html.H2(id = 'current-date', style={'display': 'inline-block', 'padding-left': '30px'}),
    ], style = {"border":"2px #317eab solid",'backgroundColor':'#c2e3f7', 'padding-top': '10px', 'padding-bottom': '10px'}),
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
          Output('current-time', 'children')
          ],
          [Input('interval-component', 'n_intervals')],
          prevent_initial_call=True)
def update_metrics(n_intervals):

    mydb = mysql.connector.connect(host=setup.host, user=setup.user, password=setup.password, database=setup.database, port=setup.port)
    mycursor = mydb.cursor()

    # Obtain updated job and employee dataframes
    mycursor.execute("SELECT JOB_ID, START, STOP, DURATION, MAX_NO_FTES, EMPLOYEES_PER_HOUR, TOTAL_FTE_HOURS, BREAK_TIME FROM " + setup.job_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
    job_data = mycursor.fetchall()
    df1 = pd.DataFrame(job_data, columns=["JOB_ID", "START", "STOP", "DURATION", "MAX_NO_FTES", "EMPLOYEES_PER_HOUR", "TOTAL_FTE_HOURS", "BREAK_TIME"])

    mycursor.execute("SELECT EMPLOYEE_ID, START, STOP, DURATION FROM " + setup.employee_table + " WHERE STOP IS NULL AND STATION=%s", ([setup.Location_name]))
    employee_data = mycursor.fetchall()
    df2 = pd.DataFrame(employee_data, columns=["EMPLOYEE_ID", "START", "STOP", "DURATION"])

    # Check for warnings
    check_for_warnings()

    # Update break time
    check_for_breaks()

    # Remove milliseconds from datetime value
    current_time = datetime.datetime.now()
    temp = str(current_time)[11:19] + " " + str(current_time)[0:10]
    current_time = convert_string_to_datetime(temp)
    current_time = current_time + timedelta(hours=0, minutes=0, seconds=5)

    # Update the duration of all open jobs and employees
    set_duration_of_all_data(current_time)

    # Calculate the total FTE hours for the active job
    for i in job_data:
        calculate_total_FTE_hours(i[0], setup.Location_name, i[1], i[2], current_time)

    # Calculate the number of employees per hour the active job
    calculate_no_of_employees_per_hour()

    # Data for employees over time graph
    data = total_location_FTEs_over_time()

    if (not data["Number of Employees"]):
        # Default max_value to set y-axis range
        max_value = 3
    else:
        max_value = (max(data['Number of Employees']) + 1)
    
    # Create employees over time figure
    fig = go.Figure(go.Scatter(x = data['Time'], y = data['Number of Employees'], name=setup.Location_name, mode='lines'))
    fig.update_layout(margin=dict(t=40, b=10),yaxis_range=[0,max_value],title_text=("<b>"+"Total Number of Employees over time at " + setup.Location_name + "<b>"), title_x=0.5, title_y = 0.95, xaxis_title="Time (hours)", yaxis_title="No. of Employees")
    fig.add_scatter(x=[data['Time'][-1]], y = [data['Number of Employees'][-1]], mode = 'markers+text', text = data['Number of Employees'][-1],textposition='top right', showlegend=False, line_color='rgb(0, 0, 0)')

    mycursor.close()
    mydb.close()

    current_date = date.today()
    current_time = current_time - timedelta(hours=0, minutes=0, seconds=5)
    current_time = str(current_time.time())[0:8]

    return (df1.to_dict('records'), df2.to_dict('records'), fig, current_date, current_time)

if __name__ == '__main__':
    app.run_server(debug=True, use_reloader=False)
### DASH APP ###
