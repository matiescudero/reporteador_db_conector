import sys
import json
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime


def execute_sql_query(mapstore_engine, sql_query):
    """Execute the 'tables_processing.sql' query on mapstore database.

    Args:
        mapstore_engine (sqlalchemy.engine.base.Engine): Mapstore DB sqlalchemy engine.
        sql_query (sqlalchemy.sql.elements.TextClause): 'tables_processing.sql' query
    """

    with mapstore_engine.connect().execution_options(autocommit=True) as con:
        con.execute(sql_query)
    print("[OK] - SQL query successfully executed")


def open_sql_query():
    """Open the SQL query to generate the outputs tables on mapstore database.

    Returns:
        sqlalchemy.sql.elements.TextClause
    """

    with open("./sql_queries/tables_processing.sql", encoding="utf8") as file:
        sql_query = text(file.read())
    print("[OK] - SQL file successfully opened")
    return sql_query


def create_mapstore_engine(mapstore_connection):
    """Create sqlalchemy mapstore engine based on the mapstore connection string.

    Args:
        mapstore_connection (str): string with the mapstore databse connection.

    Returns:
        sqlalchemy.engine.base.Engine
    """

    mapstore_engine = create_engine(mapstore_connection)
    print("[OK] - SQLAlchemy engine succesfully generated")
    return mapstore_engine


def create_mapstore_connection(config_data):
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
    return mapstore_connection 


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

    # Get dbs config parameters
    config = get_config(config_filepath)

    # Connect to 'mapstore' database
    mapstore_connection = create_mapstore_connection(config)

    # Create mapstore's database engine
    mapstore_engine = create_mapstore_engine(mapstore_connection)

    # Open the 'tables_processing.sql' file
    sql_query = open_sql_query()

    # Execute the SQL to preprocces the input tables
    execute_sql_query(mapstore_engine, sql_query)
    
    end = datetime.now()

    print(f"[OK] - Tables successfully copied to mapstore's database. Time elapsed: {end - start}")


if __name__ == "__main__":
    main(sys.argv)