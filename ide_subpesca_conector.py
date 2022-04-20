from pickletools import read_uint1
import requests
import sys
import json
import pandas as pd
from shapely.geometry import Polygon
from sqlalchemy import create_engine
from sqlalchemy import text
import json


def execute_sql_query(mapstore_engine, sql_query):
    with mapstore_engine.connect() as con:
        con.execute(sql_query)
    print("SQL se ejecuto")

def open_sql_query():
    with open("./sql_queries/add_geometry_centros.sql") as file:
        sql_query = text(file.read())
    return sql_query
 
def df_to_db(centros_df, config_data, mapstore_engine):
    centros_df.to_sql('concesiones_acuicultura', 
                        mapstore_engine, 
                        if_exists = 'replace', 
                        schema = config_data['mapstore']['schema'], 
                        index = False)

def create_mapstore_engine(mapstore_connection):
    mapstore_engine = create_engine(mapstore_connection)
    return mapstore_engine

def create_mapstore_connection(config_data):
    mapstore_connection = 'postgresql://{}:{}@{}:{}/{}'.format(
        config_data['mapstore']['user'],
        config_data['mapstore']['passwd'], 
        config_data['mapstore']['host'], 
        config_data['mapstore']['port'], 
        config_data['mapstore']['db'])
    return mapstore_connection   

def drop_str_geometry(centros_df):
    centros_df.drop('geometry.rings', axis=1, inplace=True)
    return centros_df

def transform_geometry_column(centros_df):
    centros_df["geometry"] = centros_df["geometry"].apply(Polygon).apply(str)
    return centros_df

def polygon_coords_to_df(centros_df, coord_list):
    centros_df["geometry"] = coord_list
    return centros_df

def list_to_tuples(centros_df):
    coord_list = [[*map(tuple, row[0])] for row in centros_df["geometry.rings"].values]
    return coord_list

def rename_df_columns(centros_df):
    centros_df = centros_df.rename(columns = lambda row: row.lstrip('attributes.'))
    return centros_df

def json_to_df(json_response):
    centros_df = pd.json_normalize(json_response['features'])
    return centros_df

def response_to_json(ide_request):
    json_response = ide_request.json()
    return json_response

def get_ide_request(config_data):
    ide_request = requests.get(config_data["ide_subpesca"]["request_url"], headers = config_data["ide_subpesca"]["headers"])
    return ide_request

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
        config_data = json.load(json_file)

    if config_data == {}:
        sys.exit("[ERROR] - Config file is empty.")

    return config_data


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
    # Get parameters
    config_filepath = get_parameters(argv)

    # Get dbs config parameters
    config_data = get_config(config_filepath)

    # 
    ide_request = get_ide_request(config_data)

    #
    json_response = response_to_json(ide_request)

    # 
    centros_df = json_to_df(json_response)
    
    # 
    centros_df = rename_df_columns(centros_df)

    #
    coord_list = list_to_tuples(centros_df)

    #
    centros_df = polygon_coords_to_df(centros_df, coord_list)

    #
    centros_df = transform_geometry_column(centros_df)

    #
    centros_df = drop_str_geometry(centros_df)

    #
    mapstore_connection = create_mapstore_connection(config_data)

    #
    mapstore_engine = create_mapstore_engine(mapstore_connection)

    #
    df_to_db(centros_df, config_data, mapstore_engine)

    #
    sql_query = open_sql_query()

    #
    execute_sql_query(mapstore_engine, sql_query)


if __name__ == "__main__":
    main(sys.argv)

