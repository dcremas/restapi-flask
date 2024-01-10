import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

dbname = os.getenv('PG_DB')
user = os.getenv('PG_USERNAME')
password = os.getenv('PG_PASSWORD')
host = os.getenv('PG_HOST')
port =os.getenv('PG_PORT')

url_string = f"dbname={dbname} user={user} password={password} host={host} port={port}"

connection = psycopg2.connect(url_string)

stations_input = ['70381025309', '72290023188', '72530094846', '72494023234', '72565003017',
                  '91182022521', '72509014739', '72606014764', '72306013722', '74486094789']

stations_tuple = tuple(stations_input)


def get_all_observations_sample():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f'''
SELECT
    obs.station,
    loc.station_name,
    loc.state,
    obs.date,
    EXTRACT(YEAR from obs.date) AS rdg_year,
    EXTRACT(MONTH from obs.date) AS rdg_month,
    EXTRACT(DAY from obs.date) AS rdg_day,
    obs.tmp,
    obs.slp,
    obs.wnd,
    obs.prp,
    obs.dew
FROM observations obs
JOIN locations loc
    ON obs.station = loc.station
JOIN regions rgn
    ON loc.state = rgn.state
WHERE obs.station IN {stations_tuple}
    AND EXTRACT(YEAR from obs.date) BETWEEN 2023 AND 2023
    AND obs.source IN ('6', '7')
    AND obs.report_type IN ('FM-15')
    AND obs.slp BETWEEN 20.00 AND 35.00
    AND obs.prp <= 10.00
ORDER BY obs.station, obs.date
LIMIT 100;
'''
                )
            observations = cursor.fetchall()
            if observations:
                result = []
                for observation in observations:
                    result.append({
                        "station": observation[0],
                        "station_name": observation[1],
                        "state": observation[2],
                        "date": observation[3],
                        "rdg_year": observation[4],
                        "rdg_month": observation[5],
                        "rdg_day": observation[6],
                        "tmp": observation[7],
                        "slp": observation[8],
                        "wnd": observation[9],
                        "dew": observation[10],                        
                        })
                return result
            else:
                return f"Error, Observations not found, 404."


def get_all_stations():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f'''
SELECT
    rgn.region,
    rgn.sub_region,
    loc.state,
    obs.station,
    loc.station_name
FROM observations obs
JOIN locations loc
    ON obs.station = loc.station
JOIN regions rgn
    ON loc.state = rgn.state
WHERE obs.station IN {stations_tuple}
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 5;
'''
                ) 
            stations = cursor.fetchall()
            if stations:
                result = []
                for station in stations:
                    result.append({
                        "region": station[0],
                        "sub_region": station[1],
                        "state": station[2],
                        "station": station[3],
                        "station_name": station[4],
                         })
                return result
            else:
                return f"Error, Stations not found, 404."


def get_all_dates():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f'''
SELECT
    EXTRACT(YEAR from obs.date) AS rdg_year,
    EXTRACT(MONTH from obs.date) AS rdg_month,
    EXTRACT(DAY from obs.date) AS rdg_day,
    DATE(obs.date) AS date
FROM observations obs
WHERE obs.station IN {stations_tuple}
    AND EXTRACT(YEAR from obs.date) BETWEEN 2023 AND 2023
    AND obs.source IN ('6', '7')
    AND obs.report_type IN ('FM-15')
    AND obs.slp BETWEEN 20.00 AND 35.00
    AND obs.prp <= 10.00
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3;
'''
                ) 
            dates = cursor.fetchall()
            if dates:
                result = []
                for date in dates:
                    result.append({
                        "rdg_year": date[0],
                        "rdg_month": date[1],
                        "rdg_day": date[2],
                        "date": date[3],
                         })
                return result
            else:
                return f"Error, Dates not found, 404."


def get_all_stationid(stationid):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f'''
SELECT
    obs.station,
    loc.station_name,
    loc.state,
    obs.date,
    EXTRACT(YEAR from obs.date) AS rdg_year,
    EXTRACT(MONTH from obs.date) AS rdg_month,
    EXTRACT(DAY from obs.date) AS rdg_day,
    obs.tmp,
    obs.slp,
    obs.wnd,
    obs.prp,
    obs.dew
FROM observations obs
JOIN locations loc
    ON obs.station = loc.station
JOIN regions rgn
    ON loc.state = rgn.state
WHERE obs.station = %s
    AND EXTRACT(YEAR from obs.date) BETWEEN 2023 AND 2023
    AND obs.source IN ('6', '7')
    AND obs.report_type IN ('FM-15')
    AND obs.slp BETWEEN 20.00 AND 35.00
    AND obs.prp <= 10.00
ORDER BY obs.station, obs.date;
'''
            , (stationid,)) 
            observations = cursor.fetchall()
            if observations:
                result = []
                for observation in observations:
                    result.append({
                         "station": observation[0],
                        "station_name": observation[1],
                        "state": observation[2],
                        "date": observation[3],
                        "rdg_year": observation[4],
                        "rdg_month": observation[5],
                        "rdg_day": observation[6],
                        "tmp": observation[7],
                        "slp": observation[8],
                        "wnd": observation[9],
                        "dew": observation[10],
                        })
                return result
            else:
                return f"Error, Observations with Station ID {stationid} not found, 404."


def get_all_date(date):
    year = int(date[:4])
    month = int(date[5:7])
    day = int(date[8:])
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                 f'''
SELECT
    obs.station,
    loc.station_name,
    loc.state,
    obs.date,
    EXTRACT(YEAR from obs.date) AS rdg_year,
    EXTRACT(MONTH from obs.date) AS rdg_month,
    EXTRACT(DAY from obs.date) AS rdg_day,
    obs.tmp,
    obs.slp,
    obs.wnd,
    obs.prp,
    obs.dew
FROM observations obs
JOIN locations loc
    ON obs.station = loc.station
JOIN regions rgn
    ON loc.state = rgn.state
WHERE obs.station IN {stations_tuple}
    AND EXTRACT(year from date) = {year}
    AND EXTRACT(month from date) = {month}
    AND EXTRACT (day from date) = {day}
    AND obs.source IN ('6', '7')
    AND obs.report_type IN ('FM-15')
    AND obs.slp BETWEEN 20.00 AND 35.00
    AND obs.prp <= 10.00
ORDER BY obs.station, obs.date;
'''
                 )
            observations = cursor.fetchall()
            if observations:
                result = []
                for observation in observations:
                    result.append({
                         "station": observation[0],
                        "station_name": observation[1],
                        "state": observation[2],
                        "date": observation[3],
                        "rdg_year": observation[4],
                        "rdg_month": observation[5],
                        "rdg_day": observation[6],
                        "tmp": observation[7],
                        "slp": observation[8],
                        "wnd": observation[9],
                        "dew": observation[10],
                        })
                return result
            else:
                return f"Error, records with Date {date} not found, 404."
