import sqlite3
from sqlite3 import Error
from utils import *
import pandas as pd

def main():
    database = "air_quality_monitoring.db"

    sql_create_measures_table = """CREATE TABLE IF NOT EXISTS measures (
                                    sid varchar(100) PRIMARY KEY,
                                    id varchar(100) NOT NULL,
                                    position integer NOT NULL,
                                    created_at integer NOT NULL,
                                    created_meta NULL,
                                    updated_at integer NOT NULL,
                                    updated_meta NULL,
                                    meta varchar(100) NOT NULL,
                                    measureid varchar(100) NOT NULL,
                                    measurename varchar(100) NOT NULL,
                                    measuretype varchar(100) NOT NULL,
                                    stratificationlevel varchar(100) NOT NULL,
                                    statefips varchar(100) NOT NULL,
                                    statename varchar(100) NOT NULL,
                                    countyfips varchar(100) NOT NULL,
                                    countyname varchar(100) NOT NULL,
                                    reportyear varchar(100) NOT NULL,
                                    value varchar(100) NOT NULL,
                                    unit varchar(100) NOT NULL,
                                    unitname varchar(100) NOT NULL,
                                    dataorigin varchar(100) NOT NULL,
                                    monitoronly varchar(100) NOT NULL
                                );"""
    
    # get dataset
    print('\nDownloading dataset...')
    dataset_url_path = 'https://data.cdc.gov/api/views/cjae-szjv/rows.json?accessType=DOWNLOAD'
    dataset = get_dataset(dataset_url_path)
    column_names = [column['fieldName'].replace(':', '') for column in dataset['meta']['view']['columns']]
    print('\n')
    
    # create database connection
    air_quality_db = create_connection(database)

    # create table
    if air_quality_db is not None:
        create_table(air_quality_db, sql_create_measures_table)
    else:
        print("An error occurred: no database connection!")
        
    # insert measures   
    print('\nInserting measures...')
    df = pd.DataFrame(dataset['data'], columns=column_names)
    try:
        df.to_sql(name="measures", con=air_quality_db, if_exists="replace", chunksize=1000)
        inserted_count = df.shape[0]
        print('\n%s measures successfully inserted.' %inserted_count)
    except Error as e:
        print("An error occurred:",e)
    
    air_quality_db.close()
    print('\n%s database closed' %database)
    
    
if __name__ == '__main__':
    main()
