import mysql.connector
import pandas as pd
from utils import fetch_airport, fetch_airline, create_timestamps, create_arr_record, create_dep_record, create_fs_record, create_delay_record

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Epcot2024!",
    database="mydatabase"
)
mycursor = mydb.cursor()

query = "INSERT INTO airports (iata_code, airport_name) VALUES (%s, %s)"
flights = pd.read_csv("flights.csv", keep_default_na=False)

flights["CANCELLED"] = flights["CANCELLED"].astype(int).map({1: True, 0: False})
flights["DIVERTED"] = flights["DIVERTED"].astype(int).map({1: True, 0: False})
flights["DEP_DEL15"] = pd.to_numeric(flights["DEP_DEL15"], errors="coerce").fillna(0).astype(int).map({1: True, 0: False})
flights["ARR_DEL15"] = pd.to_numeric(flights["ARR_DEL15"], errors="coerce").fillna(0).astype(int).map({1: True, 0: False})
flights["DEP_DELAY"] = pd.to_numeric(flights["DEP_DELAY"], errors="coerce").fillna(0).astype(int)
flights["ARR_DELAY"] = pd.to_numeric(flights["ARR_DELAY"], errors="coerce").fillna(0).astype(int)
flights["DEST_STATE_FIPS"] = pd.to_numeric(flights["DEST_STATE_FIPS"], errors="coerce").fillna(0).astype(int)
flights["ORIGIN_STATE_FIPS"] = pd.to_numeric(flights["ORIGIN_STATE_FIPS"], errors="coerce").fillna(0).astype(int)
flights["CRS_ELAPSED_TIME"] = pd.to_numeric(flights["CRS_ELAPSED_TIME"], errors="coerce").fillna(0).astype(int)
flights["ACTUAL_ELAPSED_TIME"] = pd.to_numeric(flights["ACTUAL_ELAPSED_TIME"], errors="coerce").fillna(0).astype(int)
flights["FLIGHTS"] = pd.to_numeric(flights["FLIGHTS"], errors="coerce").fillna(0).astype(int)
flights["DISTANCE"] = pd.to_numeric(flights["DISTANCE"], errors="coerce").fillna(0).astype(int)
flights["DISTANCE_GROUP"] = pd.to_numeric(flights["DISTANCE_GROUP"], errors="coerce").fillna(0).astype(int)
flights["CARRIER_DELAY"] = pd.to_numeric(flights["CARRIER_DELAY"], errors="coerce").fillna(0).astype(int)
flights["WEATHER_DELAY"] = pd.to_numeric(flights["WEATHER_DELAY"], errors="coerce").fillna(0).astype(int)
flights["NAS_DELAY"] = pd.to_numeric(flights["NAS_DELAY"], errors="coerce").fillna(0).astype(int)
flights["SECURITY_DELAY"] = pd.to_numeric(flights["SECURITY_DELAY"], errors="coerce").fillna(0).astype(int)
flights["LATE_AIRCRAFT_DELAY"] = pd.to_numeric(flights["LATE_AIRCRAFT_DELAY"], errors="coerce").fillna(0).astype(int)

for index, row in flights.iterrows():
    origin = fetch_airport(mycursor, row['ORIGIN'])
    dest = fetch_airport(mycursor, row['DEST'])
    airline = fetch_airline(mycursor, row['OP_CARRIER'])

    query = ("INSERT INTO flights (origin_airport, origin_state, dest_airport, dest_state, airline_id, flight_number, cancelled, diverted) VALUES "
             "(%s, %s, %s, %s, %s, %s, %s, %s)")
    vals = (origin[0], row["ORIGIN_STATE_FIPS"], dest[0], row["DEST_STATE_FIPS"], airline[0], row['OP_CARRIER_FL_NUM'], row['CANCELLED'], row['DIVERTED'])
    mycursor.execute(query, vals)
    mydb.commit()
    flight_id = mycursor.lastrowid

    create_fs_record(mycursor, mydb, row, flight_id)
    create_delay_record(mycursor, mydb, row, flight_id)

    if not row["CANCELLED"] and not row["DIVERTED"]: # Normal Flight
        times = create_timestamps(row['MONTH'], row['DAY_OF_MONTH'], row['YEAR'], row['DEP_TIME'], row['ARR_TIME'])
        crs_times = create_timestamps(row['MONTH'], row['DAY_OF_MONTH'], row['YEAR'], row['CRS_DEP_TIME'], row['CRS_ARR_TIME'])
        create_dep_record(mycursor, mydb, crs_times, times, flight_id, row)
        create_arr_record(mycursor, mydb, crs_times, times, flight_id, row)
    elif row["CANCELLED"]: # Cancelled Flight
        times = (None, None)
        crs_times = (None, None)
    else: # Diverted Flight
        times = create_timestamps(row['MONTH'], row['DAY_OF_MONTH'], row['YEAR'], row['DEP_TIME'], None)
        crs_times = create_timestamps(row['MONTH'], row['DAY_OF_MONTH'], row['YEAR'], row['CRS_DEP_TIME'], None)
        create_dep_record(mycursor, mydb, crs_times, times, flight_id, row)

    if (index % 500 == 0):
        print(str(index) + " rows processed")