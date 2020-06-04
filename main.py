import psycopg2
from time import sleep
from datetime import datetime
import pandas as pd
import json
import logging
import os
from helpers.pandas_sql_helpers import import_data_to_sql_tab
from helpers.pandas_sql_helpers import import_data_to_sql_fwf
from helpers.pandas_sql_helpers import import_data_to_sql_csv_gz
from helpers.pandas_sql_helpers import stream_dataframe_to_postgres_table
from io import StringIO


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)



sleep(10)
connection=psycopg2.connect(user='user',
                        password='password',
                        host='localhost',
                        database='database')

connection.autocommit = True
cursor = connection.cursor()
# Print PostgreSQL Connection properties
logging.info('connecetion parameters: {parameters}'.format(parameters=connection.get_dsn_parameters()))


cursor.execute('DROP SCHEMA IF EXISTS tr CASCADE;')
cursor.execute('CREATE SCHEMA IF NOT EXISTS tr')

# city data

url = "http://www.fa-technik.adfc.de/code/opengeodb/DE.tab"
df,create_statement=import_data_to_sql_tab(url,'tr.stadt')

cursor.execute(create_statement)

stream_dataframe_to_postgres_table(connection,df,'tr.stadt')

# station data


url = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

headers=['ID'
,'LATITUDE'
,'LONGITUDE'
,'ELEVATION'
,'STATE'
,'NAME'
,'GSN FLAG'
,'HCN/CRN FLAG'
,'WMO ID']   
df,create_statement= import_data_to_sql_fwf(url,headers,'tr.stations')

cursor.execute(create_statement)

stream_dataframe_to_postgres_table(connection,df,'tr.stations')


# yearly data

years=['2018','2020']

df_final=pd.DataFrame()

for year in years:
    url=f"https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz".format(year=year)

    headers=['ID'
    ,'YEAR/MONTH/DAY'
    ,'ELEMENT'
    ,'DATA VALUE'
    ,'M-FLAG'
    ,'Q-FLAG'
    ,'S-FLAG'
    ,'OBS-TIME']

    table_name='tr.yearly_data'

    df,create_statement= import_data_to_sql_csv_gz(url,headers,table_name,timeout=120)

    df_final=df_final.append(df, ignore_index=True)
    
cursor.execute(create_statement)

stream_dataframe_to_postgres_table(connection,df_final,table_name)


sqlfile = open('sql/DML/stations_5km.sql', 'r')
cursor.execute(sqlfile.read())
logging.info('Creating table stations_5km')
sqlfile = open('sql/DML/max_temp.sql', 'r')
cursor.execute(sqlfile.read())
logging.info('Creating table max_temp')
sqlfile = open('sql/DML/median_temperatures_city.sql', 'r')
cursor.execute(sqlfile.read())
logging.info('Creating table median_temperatures_city')
sqlfile = open('sql/DML/median_temperatures_germany.sql', 'r')
cursor.execute(sqlfile.read())
logging.info('Creating table median_temperatures_germany')
sqlfile = open('sql/DML/hotness_score.sql', 'r')
cursor.execute(sqlfile.read())
logging.info('Creating table hotness_score')
sqlfile = open('sql/DML/percentile_city.sql', 'r')
cursor.execute(sqlfile.read())
logging.info('Creating table percentile_city')

