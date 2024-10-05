import os
from datetime import datetime
import pandas as pd 
import numpy as np
import time


file_directory = r'C:\Users\moufi\Desktop\Corteva Project\remaster\Testing Folder'

file_names = os.listdir(file_directory)

cols = ['max_temp', 'min_temp', 'precipitation']

for file in file_names:
    # Begin measuring the code execution time.

    start_time = time.time()
    # Station name is the name of the file.
    station_name = os.path.splitext(os.path.basename(file))[0]

    weather_dataset = pd.read_csv(f"{file_directory}" + "\\" + f"{file}", header=None, sep= '\t', names=['date', 'max_temp', 'min_temp', 'precipitation'], index_col=False,)

    weather_dataset['weather_station_id'] = station_name
    weather_dataset['record_id'] = weather_dataset['weather_station_id'].map(str) + weather_dataset['date'].map(str)
    weather_dataset[cols] = weather_dataset[cols].replace({-9999: np.nan, '-9999': np.nan})
    weather_dataset = weather_dataset[['record_id', 'date', 'weather_station_id','max_temp', 'min_temp', 'precipitation' ]]




    