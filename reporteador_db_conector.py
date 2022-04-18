import sys
import json
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
from datetime import datetime

#python file with SP's querys
import sql_querys as querys


def dfs_to_bd(config, mapstore_engine, df_areas_psmb, df_centros_psmb, df_existencias, df_salmonidos, df_estaciones):
    """Copy the pandas Dataframes to the 'entradas' schema in the mapstore database. 

    Args:
        config (dict): Dictionary with the config.json file information.
        mapstore_engine (sqlalchemy.engine.base.Engine.): SQL Alchemy connection engine.
        df_areas_psmb (pandas DataFrame): Dataframe with information of PSMB areas.
        df_centros_psmb (pandas DataFrame): Dataframe with information of PSMB centers.
        df_existencias (pandas DataFrame): Dataframe with information of tons of mollusk.
        df_salmonidos (pandas DataFrame): Dataframe with information of tons of salmon.
        df_estaciones (pandas DataFrame): Dataframe with PSMB stations.
    
    """

    df_areas_psmb.to_sql('areas_psmb', mapstore_engine, schema = config['mapstore']['schema'], if_exists = 'replace', index = False)
    df_centros_psmb.to_sql('centros_psmb', mapstore_engine, schema = config['mapstore']['schema'], if_exists = 'replace', index = False)
    df_existencias.to_sql('existencias_moluscos', mapstore_engine, schema = config['mapstore']['schema'], if_exists = 'replace', index = False)
    df_salmonidos.to_sql('existencias_salmonidos', mapstore_engine, schema = config['mapstore']['schema'], if_exists = 'replace', index = False)
    df_estaciones.to_sql('estaciones', mapstore_engine, schema = config['mapstore']['schema'], if_exists = 'replace', index = False)


def tables_to_df(reporteador_connection, query_file):
    """Read the input SQL querys and execute 'reporteador' database SP's and store them as pandas DFs. 

    Args:
        reporteador_connection (sqlalchemy.engine.base.Engine): SQL Alchemy connection engine
        query_file (.py file): Python file with the executable querys 
    
    Returns:
        pandas Dataframes.

    Raises:
        UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother 
        DBAPI2 objects are not tested, please consider using SQLAlchemy
    """

    df_areas_psmb = pd.read_sql(query_file.areas_psmb, reporteador_connection)
    df_centros_psmb = pd.read_sql(query_file.centros_psmb, reporteador_connection)
    df_existencias = pd.read_sql(query_file.existencias, reporteador_connection)
    df_salmonidos = pd.read_sql(query_file.salmonidos, reporteador_connection)
    df_estaciones = pd.read_sql(query_file.estaciones, reporteador_connection)

    print("[Ok] - SP's executed and stored successfully")

    return df_areas_psmb, df_centros_psmb, df_existencias, df_salmonidos, df_estaciones

def create_mapstore_engine(mapstore_connection):
    """Creates the SQL Alchemy engine to access to the Mapstore database's tables. 

    Args:
        mapstore_connection (string):  Mapstore database connection string.
    
    Returns:
        sqlalchemy.engine.base.Engine.
    """

    mapstore_engine = create_engine(mapstore_connection)

    print("[OK] - Mapstore engine successfully created")

    return mapstore_engine

def connect_mapstore_db(config):
    """Creates the Mapstore database connection string based on the config parameters. 

    Args:
        config (dict):  Dictionary with the config.json file information.
    
    Returns:
        string: Connection string to create a SQL Alchemy engine.
    """

    mapstore_connection = 'postgresql://{}:{}@{}:{}/{}'.format(
        config['mapstore']['user'],
        config['mapstore']['passwd'], 
        config['mapstore']['host'], 
        config['mapstore']['port'], 
        config['mapstore']['db'])

    print("[OK] - Connection string successfully created")

    return mapstore_connection

def connect_reporteador_db(config):
    """Generate a pyodbc connection based on the 'reporteador' database parameters.

    Args:
        config (dict):  Dictionary with the config.json file information.
    
    Returns:
        pyodbc.Connection

    Notes:
        The 'DRIVER' parameter changes based on the installed ODBC driver.  
    """

    reporteador_connection = pyodbc.connect('DRIVER={SQL Server};SERVER=' + 
    config['reporteador']['server'] + ';DATABASE=' + 
    config['reporteador']['db'] +';UID=' + 
    config['reporteador']['user'] + ';pwd=' + 
    config['reporteador']['passwd'])

    print("[OK] - Reporteador database's pyodbc connection successfully generated")

    return reporteador_connection

def get_config(filepath=""):
    """Read the config.json file.

    Args:
        filepath (string):  config.json file path
    
    Returns:
        dict
    """

    if filepath == "":
        sys.exit("[ERROR] - Config filepath empty.")

    with open(filepath) as json_file:
        data = json.load(json_file)

    if data == {}:
        sys.exit("[ERROR] - Config file is empty.")

    return data

def get_parameters(argv):
    """Store the input parameters.

    Args:
        argv (list):  input parameters
    
    Returns:
        string: config.json path
    """

    config_filepath = argv[1]
    return config_filepath

def main(argv):
    start = datetime.now()

    #Get parameters
    config_filepath = get_parameters(argv)

    #Get dbs config parameters
    config = get_config(config_filepath)

    #Connect to 'reporteador' database
    reporteador_connection = connect_reporteador_db(config)

    #Connect to 'mapstore' database
    mapstore_connection = connect_mapstore_db(config)

    #Create mapstore's database engine
    mapstore_engine = create_mapstore_engine(mapstore_connection)

    #Execute 'reporteador' database SP's and store them as pandas DFs
    df_areas_psmb, df_centros_psmb, df_existencias, df_salmonidos, df_estaciones = tables_to_df(reporteador_connection, querys)

    #Pandas DFs to mapstore database
    dfs_to_bd(config, mapstore_engine, df_areas_psmb, df_centros_psmb, df_existencias, df_salmonidos, df_estaciones)

    end = datetime.now()

    print(f"[OK] - Tables successfully copied to mapstore's database. Time elapsed: {end - start}")

if __name__ == "__main__":
    main(sys.argv)

