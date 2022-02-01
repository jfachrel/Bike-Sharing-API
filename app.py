import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__)

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    trips = get_trip_id(trip_id, conn)
    return trips.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/average_duration_trips')
def route_average_duration_trips():
    conn = make_connection()
    trips = get_avg_duration_trips(conn)
    return trips.to_json()

@app.route('/trips/average_duration/<bike_id>')
def route_avg_duration_by_id(bike_id):
    conn = make_connection()
    trips = get_avg_duration_by_id(bike_id,conn)
    return trips.to_json()

@app.route('/rent_activities_in_period', methods=['POST'])
def route_rent_activities_in_period():
    input_data = request.get_json(force=True) # Get the input as dictionary
    specified_date = input_data['period'] # Select specific items (period) from the dictionary

    conn = make_connection()
    query = f"""
    SELECT * FROM trips
    WHERE start_time LIKE ({'"'+specified_date+'%'+'"'})"""
    
    selected_data = pd.read_sql_query(query, conn)

    # Make the aggregate
    result = selected_data.groupby('start_station_id').agg({
        'bikeid' : 'count', 
        'duration_minutes' : 'mean'
    })

    # Return the result
    return result.to_json()

@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

# Functions
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'Successful!'

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'Successful!'

def get_avg_duration_trips(conn):
    query = f"""
    SELECT start_station_name, avg(duration_minutes) as avg_duration_minutes
    FROM trips
    GROUP BY start_station_name"""
    result = pd.read_sql_query(query, conn)
    return result

def get_avg_duration_by_id(bike_id,conn):
    query = f"""
    SELECT bikeid, avg(duration_minutes) as avg_duration_minutes
    FROM trips
    WHERE bikeid = {'"'+bike_id+'"'}
    """
    result = pd.read_sql_query(query, conn)
    return result

if __name__ == '__main__':
    app.run(debug=True, port=5000)