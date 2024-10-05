from flask import Flask, render_template
import sqlite3

app = Flask(__name__)

# SQLite connection function
def get_db_connection():
    conn = sqlite3.connect('corteva_database.db')
    conn.row_factory = sqlite3.Row
    return conn

# Endpoint to display weather data in an HTML page
@app.route('/weather_html', methods=['GET'])
def display_weather_html():
    connection = get_db_connection()
    weather_data = connection.execute('SELECT * FROM Weather_Table').fetchall()
    connection.close()

    return render_template('weather.html', weather_data=weather_data)

@app.route('/yield_html', methods=['GET'])
def display_yield_data():
    connection = get_db_connection()
    yield_data = connection.execute('SELECT * FROM Yield_Table').fetchall()
    connection.close()

    return render_template('yield.html', yield_data = yield_data)
    
@app.route('/logs_html', methods=['GET'])
def display_logs_data():
    connection = get_db_connection()
    logs_data = connection.execute('SELECT * FROM Logs_Table').fetchall()

    connection.close()
    return render_template('logs.html', logs_data=logs_data)

@app.route('/stats_html', methods=['GET'])
def display_stat_data():
    connection = get_db_connection()
    stats_data = connection.execute('SELECT * FROM Weather_Stats').fetchall()
    connection.close()

    return render_template('stats.html', stats_data = stats_data)




if __name__ == '__main__':
    app.run(debug=True)