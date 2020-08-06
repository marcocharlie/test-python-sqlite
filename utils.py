import sqlite3
from sqlite3 import Error
import requests
import pandas as pd


def get_dataset(path):
    """
    A function to download a .json dataset from a url.
    """
    try:
        response = requests.get(path)
        dataset = response.json()
        print("Dataset from %s successfully loaded." %path)
        return dataset
    except Exception as e:
        print(e)
    

def create_connection(path):
    """ 
    A function to create a connection to the SQLite database specified by path.
    """
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful.")
    except Error as e:
        print(e)
    return connection
    
    
def create_table(connection, create_table_sql):
    """ 
    A function to create a table from the create_table_sql statement
    """
    try:
        c = connection.cursor()
        c.execute(create_table_sql)
        print("SQLite table creation successful.")
    except Error as e:
        print(e)
        
        
#def create_measure(connection, record):
#    """
#    A function to create a new measure in order to be interted into the measures table.    
#    The  lastrowid attribute of the Cursor object returns the generated id.
#    """
#    sql = ''' INSERT INTO measures(
#                            sid,
#                            id,
#                            position,
#                            created_at,
#                            created_meta,
#                            updated_at,
#                            updated_meta,
#                            meta,
#                            measureid,
#                            measurename,
#                            measuretype,
#                            stratificationlevel,
#                            statefips,
#                            statename,
#                            countyfips,
#                            countyname,
#                            reportyear,
#                            value,
#                            unit,
#                            unitname,
#                            dataorigin,
#                            monitoronly
#                            )
#              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
#    cursor = connection.cursor()
#    cursor.execute(sql, record)
#    connection.commit()
#    return cursor.lastrowid
#
#
#def insert_measure(database, measure):
#    """
#    A function to insert a single measure.
#    """
#    connection = create_connection(database)
#    with connection:
#        
#        # insert measure
#        try:
#            new_measure = create_measure(connection, measure)
#            print("Measured with sid %s successfully inserted" %measure[0])
#        except Error as e:
#            print(e)


### Queries

def get_total_days_concentration_by_year(database, table):
    """
    1. Sum value of "Number of days with maximum 8-hour average ozone concentration over the 
    National Ambient Air Quality Standard" per year
    """
    connection = create_connection(database)
    
    cursor = connection.cursor()
    
    query = '''
            SELECT measures.reportyear AS reportyear, 
                SUM(CAST(measures.value AS INT)) AS total_year_value, 
                measures.measureid AS measureid, 
                measures.measurename AS measurename
            FROM measures
            WHERE (measurename = "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard")
            GROUP BY reportyear
            ORDER BY reportyear ASC;
            '''
    
    cursor.execute(query)

    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows, columns=['Year', 'Total', 'Measure Id', 'Measure Name'])
    
    return df


def get_year_with_max_concentration(database, table, from_date):
    """
    2. Year with max value of "Number of days with maximum 8-hour average ozone concentration over the 
    National Ambient Air Quality Standard" from year 2008 and later(inclusive)
    """
    connection = create_connection(database)
    
    cursor = connection.cursor()
    
    query = '''
            SELECT CAST(measures.reportyear AS INT) AS reportyear, 
                MAX(CAST(measures.value AS INT)) AS max_year_value, 
                measures.measureid AS measureid, 
                measures.measurename AS measurename
            FROM measures
            WHERE (measurename = "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard") AND (reportyear >= %s)
            GROUP BY reportyear
            ORDER BY max_year_value DESC
            LIMIT 1;
            ''' %from_date
    
    cursor.execute(query)

    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows, columns=['Year', 'Max', 'Measure Id', 'Measure Name'])
    
    return df


def get_state_with_max_concentration(database, table):
    """
    3. Max value of each measurement per state
    """
    connection = create_connection(database)
    
    cursor = connection.cursor()
    
    query = '''
            SELECT measures.statename AS statename,
                MAX(CAST(measures.value AS INT)) AS max_value, 
                measures.measureid AS measureid, 
                measures.measurename AS measurename,
                measures.reportyear AS reportyear
            FROM measures
            GROUP BY statename, measureid
            ORDER BY statename ASC;
            '''
    
    cursor.execute(query)

    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows, columns=['State', 'Max', 'Measure Id', 'Measure Name', 'Year'])
    
    return df


def get_avg_pm25_by_year_and_state(database, table):
    """
    4. Average value of "Number of person-days with PM2.5 over the National Ambient AirQuality Standard 
    (monitor and modeled data)" per year and state in ascending order
    """
    connection = create_connection(database)
    
    cursor = connection.cursor()
    
    query = '''
            SELECT measures.reportyear AS reportyear,
                measures.statename AS statename,
                AVG(CAST(measures.value AS INT)) AS avg_value, 
                measures.measureid AS measureid, 
                measures.measurename AS measurename
            FROM measures
            WHERE (measurename = 'Number of person-days with PM2.5 over the National Ambient Air Quality Standard (monitor and modeled data)')
            GROUP BY reportyear, statename
            ORDER BY statename ASC;
            '''
    
    cursor.execute(query)

    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows, columns=['Year', 'State', 'Average', 'Measure Id', 'Measure Name'])
    df['Average'] = df['Average'].astype(int)
    
    return df


def get_stat_with_max_total_days_over_concentration(database, table):
    """
    5. State with the max accumulated value of "Number of days with maximum 8-hour average ozone 
    concentration over the National Ambient Air Quality Standard" overallyears
    """
    connection = create_connection(database)
    
    cursor = connection.cursor()
    
    query = '''
            SELECT measures.statename AS statename,
                SUM(CAST(measures.value AS INT)) AS total_value,
                measures.measureid AS measureid, 
                measures.measurename AS measurename,
                measures.reportyear AS reportyear
            FROM measures
            WHERE (measurename = 'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard')
            GROUP BY statename
            ORDER BY total_value DESC
            LIMIT 1;
            '''
    
    cursor.execute(query)

    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows, columns=['State', 'Max', 'Measure Id', 'Measure Name', 'Year'])
    
    return df


def get_avg_ozone_concentration(database, table, state):
    """
    6. Average value of "Number of person-days with maximum 8-hour average ozone concentration over the 
    National Ambient Air Quality Standard" in the state of Florida
    """
    connection = create_connection(database)
    
    cursor = connection.cursor()
    
    query = '''
            SELECT measures.statename AS statename,
                AVG(CAST(measures.value AS INT)) AS avg_value,
                measures.measureid AS measureid, 
                measures.measurename AS measurename,
                measures.reportyear AS reportyear
            FROM measures
            WHERE (measurename = 'Number of person-days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard')
                AND (statename = "%s");
            ''' %state
    
    cursor.execute(query)

    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows, columns=['State', 'Average', 'Measure Id', 'Measure Name', 'Year'])
    df['Average'] = df['Average'].astype(int)
    
    return df


def get_county_with_min_concentration_by_state_and_year(database, table):
    """
    7. Country with min "Number of days with maximum 8-hour average ozone concentratio nover the 
    National Ambient Air Quality Standard" per state per year
    """
    connection = create_connection(database)
    
    cursor = connection.cursor()
    
    query = '''
            SELECT measures.reportyear AS reportyear,
                measures.statename AS statename,
                measures.countyname AS countyname,
                MIN(CAST(measures.value AS INT)) AS min_value,
                measures.measureid AS measureid, 
                measures.measurename AS measurename
            FROM measures
            WHERE (measurename = 'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard')
            GROUP BY reportyear, statename
            ;
            ''' 
    
    cursor.execute(query)

    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows, columns=['Year', 'State', 'County', 'Min', 'Measure Id', 'Measure Name'])
    
    return df
