###################
###   General   ###
###################

Location_name = "Current Location" # Set to current station name

Locations = ["Current Location", "Location 2", "Location 3"] # Enter all locations on the shop floor

LocationsAll = ["All Locations"] + Locations # Do not modify

# Enter database table names if default names not used
scan_event = "scan_event"
job_table = "jobs"
employee_table = "employees"

#######################
###   MQTT Broker   ###
#######################

mqttBroker= "raspberrypi"

mqtt_port = 1883

########################
###   SQL Database   ###
########################

host = "127.0.0.1"

user="root"

password="winston!!2"

database="jobstafftrackingdatabase"

port = 3306
