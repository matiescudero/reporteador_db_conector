import requests
import sys
import os
import json
import pandas as pd
import logging
from shapely.geometry import Polygon
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime



def execute_sql_query(mapstore_engine, sql_query, logger):
    """Execute the 'add_geometry_centros.sql' query on mapstore database.

    Args:
        mapstore_engine (sqlalchemy.engine.base.Engine): Mapstore DB sqlalchemy engine.
        sql_query (sqlalchemy.sql.elements.TextClause): 'add_geometry_centros.sql' query
    """

    with mapstore_engine.connect().execution_options(autocommit=True) as con:
        con.execute(sql_query)
    print("[OK] - SQL query successfully executed")
    logger.debug("[OK] - EXECUTE_SQL_QUERY")

def open_sql_query(logger):
    """Open the SQL query to add the geometry type to the 'centros_acuicultura' table on mapstore database.

    Returns:
        sqlalchemy.sql.elements.TextClause
    """

    with open("./sql_queries/add_geometry_centros.sql") as file:
        sql_query = text(file.read())
    print("[OK] - SQL file successfully opened")
    logger.debug("[OK] - OPEN_SQL_QUERY")
    return sql_query
 
def df_to_db(centros_df, config_data, mapstore_engine, logger):
    """Copy the centros_df DataFrame to the mapstore database.

    Args:
        centros_df (pandas.core.frame.DataFrame): Dataframe with all the farming centers.
        config_data (dict): config.json parameters.
        mapstore_engine (sqlalchemy.engine.base.Engine): Mapstore DB sqlalchemy engine.
    
    Raises:
        SAWarning: Did not recognize type 'geometry' of column 'geom'
    """

    centros_df.to_sql('concesiones_acuicultura', 
                        mapstore_engine, 
                        if_exists = 'replace', 
                        schema = config_data['mapstore']['schema'], 
                        index = False)

    print("[OK] - DataFrame successfully copied to Mapstore database")
    logger.debug("[OK] - DF_TO_DB")

def create_mapstore_engine(mapstore_connection, logger):
    """Create sqlalchemy mapstore engine based on the mapstore connection string.

    Args:
        mapstore_connection (str): string with the mapstore databse connection.

    Returns:
        sqlalchemy.engine.base.Engine
    """

    mapstore_engine = create_engine(mapstore_connection)
    print("[OK] - SQLAlchemy engine succesfully generated")
    logger.debug("[OK] - CREATE_MAPSTORE_ENGINE")
    return mapstore_engine

def create_mapstore_connection(config_data, logger):
    """Create mapstore connection string based on the config file parameters.

    Args:
        config_data (dict): config.json parameters.

    Returns:
        str
    """

    mapstore_connection = 'postgresql://{}:{}@{}:{}/{}'.format(
        config_data['mapstore']['user'],
        config_data['mapstore']['passwd'], 
        config_data['mapstore']['host'], 
        config_data['mapstore']['port'], 
        config_data['mapstore']['db'])
    print("[OK] - Connection string successfully generated")
    logger.debug("[OK] - CREATE_MAPSTORE_CONNECTION")
    return mapstore_connection   

def drop_str_geometry(centros_df, logger):
    """Drop the previous geometry column.

    Args:
        centros_df (pandas.core.frame.DataFrame): Dataframe with all the farming centers.

    Returns:
        pandas.core.frame.DataFrame
    """

    centros_df.drop('geometry.rings', axis=1, inplace=True)
    print("[OK] - Old geometry column successfully dropped")
    logger.debug("[OK] - DROP_STR_GEOMETRY")
    return centros_df

def transform_geometry_column(centros_df, logger):
    """Transform the geometry column to a SQL readable format.

    Args:
        centros_df (pandas.core.frame.DataFrame): Dataframe with all the farming centers.

    Returns:
        pandas.core.frame.DataFrame
    """

    centros_df["geometry"] = centros_df["geometry"].apply(Polygon).apply(str)
    print("[OK] - Geometry column format successfully converted")
    logger.debug("[OK] - TRANSFORM GEOMETRY COLUMN")
    return centros_df

def polygon_coords_to_df(centros_df, coord_list, logger):
    """Appends the list of coordinates to the centros_df DataFrame as a column.

    Args:
        centros_df (pandas.core.frame.DataFrame): Dataframe with all the farming centers.
        coord_list (list): list of lists with the coodinates of each farming center.

    Returns:
        pandas.core.frame.DataFrame
    """

    centros_df["geometry"] = coord_list
    print("[OK] - New geometry column successfully appended")
    logger.debug("[OK] - POLYGON_COORDS_TO_DF")
    return centros_df

def list_to_tuples(centros_df, logger):
    """Transforms the "geometry.rings" column from list of lists to a list of tuples. 

    Args:
        centros_df (pandas.core.frame.DataFrame): Dataframe with all the farming centers.

    Returns:
        list.
    """

    coord_list = [[*map(tuple, row[0])] for row in centros_df["geometry.rings"].values]
    print("[OK] - List of lists successfully converted to list of tuples")
    logger.debug("[OK] - LIST_TO_TUPLES")
    return coord_list

def rename_df_columns(centros_df, logger):
    """Removes the word 'attributes' from the Pandas DataFrame's columns.

    Args:
        centros_df (pandas.core.frame.DataFrame): Dataframe with all the farming centers.

    Returns:
        pandas.core.frame.DataFrame.
    """

    centros_df = centros_df.rename(columns = lambda row: row.lstrip('attributes.'))
    print("[OK] - DataFrame columns successfully renamed")
    logger.debug("[OK] - RENAME_DF_COLUMNS")
    return centros_df

def json_to_df(json_response, logger):
    """Transforms the json dictionary to a Pandas DataFrame.

    Args:
        json_response (dict): Dictionary of the json response.

    Returns:
        pandas.core.frame.DataFrame.
    """
    centros_df = pd.json_normalize(json_response['features'])
    print("[OK] - JSON successfully transformed to DataFrame")
    logger.debug("[OK] - JSON_TO_DF")

    return centros_df

def response_to_json(ide_response, logger):
    """Transforms the subpesca's request to a python dictionary.

    Args:
        ide_response (requests.models.Response): response to subpesca's rest api request.

    Returns:
        dict.
    """

    json_response = ide_response.json()
    print("[OK] - IDE rest api service succesfully transformed")
    logger.debug("[OK] - RESPONSE_TO_JSON")
    return json_response

def get_ide_response(config_data, logger):
    """Gets the response of subpesca's rest api service request based on the config data parameters.

    Args:
        config_data (dict): config.json parameters.

    Returns:
        requests.models.Response.
    """

    ide_response = requests.get(config_data["ide_subpesca"]["request_url"], headers = config_data["ide_subpesca"]["headers"])
    print("[OK] - ArcGIS rest api service succesfully requested")
    logger.debug("[OK] - GET_IDE_RESPONSE")
    return ide_response

def create_logger(log_file):
    """Create a logger based on the passed log file.

    Args:
        log_file (str): Path of the log file.

    Returns:
        class logging.RootLogger.
    """
    logging.basicConfig(filename = log_file,
                    format='%(asctime)s %(message)s',
                    filemode='a')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    return logger

def create_log_file(log_path):
    """Create the log folder if not exists. Get the log file name.

    Args:
        log_path (str): Path of the log folder.

    Returns:
        str
    """
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    log_file = log_path + "/ide_subpesca_conector.log"
    return log_file  

def get_config(filepath=""):
    """Reads the config.json file.

    Args:
        filepath (string):  config.json file path.
    
    Returns:
        dict.
    """

    if filepath == "":
        sys.exit("[ERROR] - Config filepath empty.")

    with open(filepath) as json_file:
        config_data = json.load(json_file)

    if config_data == {}:
        sys.exit("[ERROR] - Config file is empty.")

    return config_data


def get_parameters(argv):
    """Stores the input parameters.

    Args:
        argv (list):  input parameters.
    
    Returns:
        string: config.json path.
    """

    config_filepath = argv[1]
    return config_filepath


def main(argv):
    start = datetime.now()

    # Get parameters
    config_filepath = get_parameters(argv)

    # Get service config parameters
    config_data = get_config(config_filepath)

    # Create the log file if not exists
    log_file = create_log_file(config_data["log_path"])

    # Create the logger
    logger = create_logger(log_file)

    # Get response from arcgis rest service
    ide_response = get_ide_response(config_data, logger)

    # Tranform response to dictionary
    json_response = response_to_json(ide_response, logger)

    # Transform dictionary to DataFrame
    centros_df = json_to_df(json_response, logger)
    
    # Rename DataFrame's columns
    centros_df = rename_df_columns(centros_df, logger)

    # Transform the list of list of coordinates to list of tuples
    coord_list = list_to_tuples(centros_df, logger)

    # Append new column to DataFrame
    centros_df = polygon_coords_to_df(centros_df, coord_list, logger)

    # Change format of the geometry column
    centros_df = transform_geometry_column(centros_df, logger)

    # Drop the old geometry column
    centros_df = drop_str_geometry(centros_df, logger)

    # Create string with the db mapstore parameters
    mapstore_connection = create_mapstore_connection(config_data, logger)

    # Create sqlalchemy engine based on the mapstore db paramters
    mapstore_engine = create_mapstore_engine(mapstore_connection, logger)

    # Copy the DataFrame to the mapstore database
    df_to_db(centros_df, config_data, mapstore_engine, logger)

    # Open the 'add_geometry_centros.sql' file
    sql_query = open_sql_query(logger)

    # Execute the SQL query to transform the geometry type of the new table
    execute_sql_query(mapstore_engine, sql_query, logger)

    end = datetime.now()

    print(f"[OK] - Table successfully copied to mapstore's database. Time elapsed: {end - start}")

if __name__ == "__main__":
    main(sys.argv)

