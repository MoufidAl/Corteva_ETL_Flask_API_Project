from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

# SQLite connection function
def get_db_connection():
    conn = sqlite3.connect('corteva_database.db')
    conn.row_factory = sqlite3.Row  # This enables name-based access to columns
    return conn

# Endpoint to fetch all records from Weather_Table
@app.route('/weather', methods=['GET'])
def get_weather_data():
    conn = get_db_connection()
    weather_data = conn.execute('SELECT * FROM Weather_Table').fetchall()
    conn.close()

    weather_list = [dict(row) for row in weather_data]
    return jsonify(weather_list)

# Endpoint to fetch a record from Yield_Table by year
@app.route('/yield/<int:year>', methods=['GET'])
def get_yield_data(year):
    conn = get_db_connection()
    yield_data = conn.execute('SELECT * FROM Yield_Table').fetchall()
    conn.close()

    yield_list = [dict(row) for row in yield_data] 
    return jsonify(dict(yield_list))

# Endpoint to fetch all logs from Logs_Table
@app.route('/logs', methods=['GET'])
def get_logs():
    conn = get_db_connection()
    logs = conn.execute('SELECT * FROM Logs_Table').fetchall()
    conn.close()

    logs_list = [dict(row) for row in logs]
    return jsonify(logs_list)

# Endpoint to fetch weather statistics by station_year_id
@app.route('/weather_stats', methods=['GET'])
def get_weather_stats(station_year_id):
    conn = get_db_connection()
    weather_stats = conn.execute('SELECT * FROM Weather_Stats').fetchall()
    conn.close()

    stats_list = [dict(row) for row in weather_stats]
    return jsonify(dict(stats_list))




if __name__ == '__main__':
    app.run(debug=True)
