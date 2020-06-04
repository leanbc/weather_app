import gzip
import io
import requests
import pandas as pd
import logging
from math import sin, cos, sqrt, atan2, radians
import urllib.request



logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)



def import_data_to_sql_tab(url,table_name):
    s = requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), sep="\t")
    df=df.rename(lambda col_name: col_name.lower() , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('#','') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace(' ','_') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('/','_') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('-','_') , axis='columns')


    logging.info('Reading {url} into DataFrame'.format(url=url))
    
    create_statement=pd.io.sql.get_schema(df,table_name)
    create_statement=create_statement.replace('"','')
    logging.info('Generating Sql from Pandas Data Frame:')
    logging.info(create_statement)
    logging.info('Returning DataFrame and SQL Create Statement')

    df=df[df['typ']=='Stadt']
    df=df.drop_duplicates(subset=['ascii'])

    logging.info('Getting only typ==Stadt and dropting duplicates in ascii column')

    return df,create_statement

def import_data_to_sql_fwf(url,headers,table_name):
    headers=map(lambda x:x.lower(),headers)
    headers=map(lambda x:x.replace(' ','_'),headers)
    headers=list(headers)

    s = requests.get(url).content
    df=pd.read_fwf(io.StringIO(s.decode('utf-8')),names=headers)
    df=df.rename(lambda col_name: col_name.lower() , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('#','') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace(' ','_') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('/','_') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('-','_') , axis='columns')


    logging.info('Reading {url} into DataFrame'.format(url=url))

    create_statement=pd.io.sql.get_schema(df,table_name)
    create_statement=create_statement.replace('"','')
    logging.info('Generating Sql from Pandas Data Frame:')
    logging.info(create_statement)
    logging.info('Returning DataFrame and SQL Create Statement')  

    return df,create_statement

def import_data_to_sql_csv_gz(url,headers,table_name,timeout=120):

    headers=map(lambda x:x.lower(),headers)
    headers=map(lambda x:x.replace(' ','_'),headers)
    headers=list(headers)


    logging.info('Requesting data from {url} '.format(url=url))
    s = requests.get(url
                     ,timeout=timeout
                     ,stream=True).content
    logging.info('Request finished')

    logging.info('Unzipping requested Data')
    gzip_file = gzip.GzipFile(fileobj=io.BytesIO(s))

    df=pd.read_csv(io.StringIO(gzip_file.read().decode('utf-8').replace('\x00','')),names=headers)
    df=column_name_transformations(df)

    logging.info('Reading data into DataFrame')
    create_statement=pd.io.sql.get_schema(df,table_name)
    create_statement=create_statement.replace('"','')
    logging.info('Generating Sql from Pandas Data Frame:')
    logging.info(create_statement)
    logging.info('Returning DataFrame and SQL Create Statement') 
    
    return df,create_statement

def stream_dataframe_to_postgres_table(connection,dataframe,table):
    
    sio = io.StringIO()
    sio.write(dataframe.to_csv(index=None, header=None, sep="\t"))
    sio.seek(0)

    logging.info('Inserting Dataframe into {table} table'.format(table=table))

    # Copy the string buffer to the database, as if it were an actual file
    with connection.cursor() as c:
        c.copy_from(sio, table, columns=dataframe.columns, sep="\t",null='')
        connection.commit()

    logging.info('Data Set Completely inserted')

def column_name_transformations(df):

    df=df.rename(lambda col_name: col_name.lower() , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('#','') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace(' ','_') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('/','_') , axis='columns')
    df=df.rename(lambda col_name: col_name.replace('-','_') , axis='columns')

    logging.info('Columns name have been transformed to be psql compliant')  

    return df

def distance_km_calculator(lat1,lon1,lat2,lon2):
    R = 6373.0
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    
    return distance