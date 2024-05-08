from datetime import datetime, timedelta
def fetch_airport(cursor, airport_code):
    query = "SELECT * FROM airports WHERE iata_code = %s"
    val = (airport_code,)

    cursor.execute(query, val)
    airport = cursor.fetchone()
    return airport

def fetch_airline(cursor, airline_code):
    query = "SELECT * FROM airlines WHERE iata_code = %s"
    val = (airline_code, )
    cursor.execute(query, val)
    airline = cursor.fetchone()
    return airline

def create_timestamps(month, day, year, dep_time, arr_time=None):
    dep_hour, dep_minute = divmod(int(dep_time), 100)
    dep_minute = int(dep_minute)
    if int(dep_hour) > 23:
        dep_hour = 0
        dep_time = datetime(year, month, day, dep_hour, dep_minute)
        dep_time = dep_time + timedelta(days=1)
    else:
        dep_time = datetime(year, month, day, dep_hour, dep_minute)

    if (arr_time != None):
        arr_hour, arr_minute = divmod(int(arr_time), 100)
        arr_minute = int(arr_minute)
        if int(arr_hour) > 23:
            arr_hour = 0
            arr_time = datetime(year, month, day, arr_hour, arr_minute)
            arr_time = arr_time + timedelta(days=1)
        else:
            arr_time = datetime(year, month, day, arr_hour, arr_minute)

        if arr_hour < dep_hour:
            arr_time = arr_time + timedelta(days=1)

    return (dep_time, arr_time)

def create_dep_record(cursor, db, crs_times, times, flight_id, row):
    query = ("INSERT INTO flight_dep_performance (crs_dep_time, dep_time, dep_delay, dep_del_15, flight_id) "
             "VALUES (%s, %s, %s, %s, %s)")
    vals = (crs_times[0], times[0], row['DEP_DELAY'], row['DEP_DEL15'], flight_id)
    cursor.execute(query, vals)
    db.commit()

def create_arr_record(cursor, db, crs_times, times, flight_id, row):
    query = ("INSERT INTO flight_arr_performance (crs_arr_time, arr_time, arr_delay, arr_del_15, flight_id) "
             "VALUES (%s, %s, %s, %s, %s)")
    vals = (crs_times[1], times[1], row['ARR_DELAY'], row['ARR_DEL15'], flight_id)
    cursor.execute(query, vals)
    db.commit()

def create_fs_record(cursor, db, row, flight_id):
    query = ("INSERT INTO flight_summaries (flight_id, crs_elapsed_time, actual_elapsed_time, flights, distance, distance_group)"
             " VALUES (%s, %s, %s, %s, %s, %s)")
    vals = (flight_id, row['CRS_ELAPSED_TIME'], row['ACTUAL_ELAPSED_TIME'], row['FLIGHTS'], row['DISTANCE'], row['DISTANCE_GROUP'])
    cursor.execute(query, vals)
    db.commit()

def create_delay_record(cursor, db, row, flight_id):
    query = ("INSERT INTO cause_of_delay (flight_id, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay)"
             " VALUES (%s, %s, %s, %s, %s, %s)")
    vals =(flight_id, row['CARRIER_DELAY'], row['WEATHER_DELAY'], row['NAS_DELAY'], row['SECURITY_DELAY'], row['LATE_AIRCRAFT_DELAY'])
    cursor.execute(query, vals)
    db.commit()