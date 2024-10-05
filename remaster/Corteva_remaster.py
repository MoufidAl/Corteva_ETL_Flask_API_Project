import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np
import time
import os

database_name = 'corteva_database.db'
weather_dirc = r'C:\Users\...\Desktop\Corteva Project\remaster....'
yield_dirc = r'C:\Users\...\Desktop\Corteva Project\remaster...'
# Creating Database Model/Schema

def create_schema(database_name):
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()  

    curr.execute('''
        CREATE TABLE IF NOT EXISTS Weather_Table (
            record_id VARCHAR(30) PRIMARY KEY NOT NULL,
            date DATE NOT NULL,
            weather_station_id VARCHAR(11) NOT NULL,
            max_temp INTEGER,
            min_temp INTEGER,
            precipitation INTEGER
        )
    ''')

    curr.execute('''
        CREATE TABLE IF NOT EXISTS Yield_Table (
            year INTEGER PRIMARY KEY NOT NULL,
            harvet INTEGER
        )
    ''')

    curr.execute('''
        CREATE TABLE IF NOT EXISTS Logs_Table (
            file_name VARCHAR(30) PRIMARY KEY NOT NULL,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            records_ingested INTEGER
        )
    ''')

    curr.execute('''
        CREATE TABLE IF NOT EXISTS Weather_Stats (
            station_year_id VARCHAR(30) PRIMARY KEY NOT NULL,
            weather_station_id VARCHAR(11) NO NULL,
            year INTEGER NO NULL,
            avg_max_temp DECIMAL (11,2),
            avg_min_temp DECIMAL (11,2),
            total_precipitation INTEGER
        )
    ''')
    connection.close()


def etl_weather(weather_dirc, database_name):
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    files = os.listdir(weather_dirc)

    cols = ['max_temp', 'min_temp', 'precipitation']

    for file in files:
        starting_time = time.time()
        station_name = os.path.splittext(os.path.basename(file)) [0]

        weather_dataset = pd.read_csv(f'{weather_dirc}' + '\\' + f'{file}', header=None, sep='\t', index_col=False, names=['date','max_temp','min_temp','preciptation'])

        weather_dataset['weather_station_id'] = station_name
        weather_dataset['record_id'] = weather_dataset['weather_station_id'].map(str) + weather_dataset['date'].map(str)
        weather_dataset[cols] = weather_dataset[cols].replace({-9999: np.nan, '-9999': np.nan})
        weather_dataset = weather_dataset[['record_id', 'date', 'weather_station_id','max_temp', 'min_temp', 'precipitation' ]]

        # Requested Change
        weather_dataset['max_temp'] = weather_dataset['max_temp'] / 10 
        weather_dataset['min_temp'] = weather_dataset['min_temp'] / 10
        weather_dataset['precipitation'] = weather_dataset['precipitation'] / 10 

        rows_ingested = len(weather_dataset.axis[0])

        # looping over each ROW in EACH file ONE AT A time. 

        for row in weather_dataset.itertuples():
            record_id = row.record_id
            date = row.date
            weather_station_id = row.weather_station_id
            max_temp = row.max_temp
            min_temp = row.min_temp
            precipitation = row.precipitation
        # Sending to Database and checking for duplicates
            duplicate_values_check = cursor.execute("""
            SELECT * FROM Weather_Table WHERE record_id = (?)""", record_id)

            if duplicate_values_check.fetchall() == []:
                cursor.execute("""
                INSERT OR IGNORE INTO Weather_Table (record_id, date, weather_station_id,max_temp,min_temp,precipitation) VALUES
                (?,?,?,?,?,?)""", (record_id,date,weather_station_id,max_temp,min_temp,precipitation))
        
        ending_time = time.time()

        cursor.execute("""INSERT OR IGNORE INTO Logs_Table(file_name, start_timne, end_time, records_ingested) VALUES
        (?,?,?,?)""", (station_name, starting_time, ending_time, rows_ingested))
    
    connection.close()

def etl_yield(yield_dirc, database_name):

    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()

    files = os.listdir(yield_dirc)

    for file in files:
        file_name = os.path.splitext(os.path.basename(file)) [0]
        starting_time = time.time()

        yield_dataset = pd.read_csv(f'{yield_dirc}' + '\\' + f'{file}', header=None, sep='\t', index_col=False, names=['year', 'harvest'])

        rows_ingested = len(yield_dataset.axis[0])

        for row in yield_dataset.itertuples():
            year = row.year
            harvest = row.harvest
        
        duplicate_values_check = cursor.execute("""
        SELECT * FROM Yield_Table WHERE year = (?)""", year)

        if duplicate_values_check.fetchall == []:
            cursor.execute("""
            INSERT OR IGNORE INTO Yield_Table (year, harvest) VALUES (?,?)""", (year, harvest))
        
        ending_time = time.time()
    cursor.execute("""
    INSERT OR IGNORE INTO Logs_Table (file_name, start_timne, end_time, records_ingested) VALUES
    (?,?,?,?)""", (file_name, starting_time, ending_time, rows_ingested))

    connection.close()

def create_statistics_table ():
    '''
    Requirements: 
    Average max temp and average min temp and total precipitation PER year PER weather station.
    '''
    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()
    
    stats_CTE = """
        SELECT
        (weather_station_id || EXTRACT (YEAR FROM DATE)) as station_year_id,
        EXTRACT (YEAR FROM DATE) as year,
        weather_station_id,
        AVG(max_temp) as avg_max_temp,
        AVG(min_temp) as avg_min_temp,
        SUM(precipitation) as total_precipitation

        FROM
            Weather_Table
        
        GROUP BY
            station_year_id, year, weather_station_id 
            """
    
    stats_query_output = cursor.execute(stats_CTE)

    statistics_dataframe = stats_query_output.fetchall()

    for i,row in enumerate(statistics_dataframe):
        cursor.execute("""INSERT OR IGNORE INTO Weather_Stats (
        station_year_id, year, weather_station_id, max_temp, min_temp, total_precipitation)
        VALUES (?,?,?,?,?,?)""", tuple(row))
    
    cursor.close()
    



    






        
