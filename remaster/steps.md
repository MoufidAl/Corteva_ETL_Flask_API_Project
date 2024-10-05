## Data Model

### Create Data Model for the Ingested Data per the below requirements

1. Create a data model for the weather data table
2. Create a data model for the yield data table
3. Create a data model for the logs data
4. create a calculation/ statistics table for some agg functions on the data.

## Data ETL
Designed ETL functions for each file and the statistics table

### Create Weather Data ETL function
Designed an ETL function for the weather files looping through each file, and extracting the required data, cleaning the data, and sending it to an sql database

### Create Yield Data ETL function
Designed an ETL function for the Yield data file. 


### Create Statistics Table
Created the Data Statistics table in SQL after uploading the data from both the weather files and the yield file. 


## Flask and Web Application 

The API will be available at http://127.0.0.1:5000/

Endpoints:
GET Weather Data:

URL: http://127.0.0.1:5000/weather
Returns all data from the Weather_Table.

GET Yield Data by Year:
URL: http://127.0.0.1:5000/yield/<year>

GET Logs:
URL: http://127.0.0.1:5000/logs
Returns all records from the Logs_Table.


GET Weather Stats by Station Year ID:
URL: http://127.0.0.1:5000/weather_stats/<station_year_id>


