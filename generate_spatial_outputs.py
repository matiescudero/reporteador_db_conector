import sys
import json
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime


def execute_sql_query(mapstore_engine, sql_query, logger):
    """Execute the 'tables_processing.sql' query on mapstore database.

    Args:
        mapstore_engine (sqlalchemy.engine.base.Engine): Mapstore DB sqlalchemy engine.
        sql_query (sqlalchemy.sql.elements.TextClause): 'tables_processing.sql' query
    """
    try:
        with mapstore_engine.connect().execution_options(autocommit=True) as con:
            con.execute(sql_query)
        print("[OK] - SQL query successfully executed")
        logger.debug("[OK] - EXECUTE_SQL_QUERY")

    except Exception as e:
        print('[ERROR] - Executing SQL query')
        logger.error('[ERROR] - EXECUTE_SQL_QUERY')
        sys.exit(2)


def open_sql_query(logger):
    """Open the SQL query to generate the outputs tables on mapstore database.

    Returns:
        sqlalchemy.sql.elements.TextClause
    """

    try:
        with open("./sql_queries/tables_processing.sql", encoding="utf8") as file:
            sql_query = text(file.read())
        print("[OK] - SQL file successfully opened")
        logger.debug("[OK] - OPEN_SQL_QUERY")
        return sql_query

    except Exception as e:
        print('[ERROR] - Opening SQL Query')
        logger.error('[ERROR] - OPEN_SQL_QUERY')
        sys.exit(2)


def create_mapstore_engine(mapstore_connection, logger):
    """Create sqlalchemy mapstore engine based on the mapstore connection string.

    Args:
        mapstore_connection (str): string with the mapstore databse connection.

    Returns:
        sqlalchemy.engine.base.Engine
    """

    try:
        mapstore_engine = create_engine(mapstore_connection)
        print("[OK] - SQLAlchemy engine succesfully generated")
        logger.debug("[OK] - CREATE_MAPSTORE_ENGINE")
        return mapstore_engine

    except Exception as e:
        print('[ERROR] - Creating Mapstore Engine')
        logger.error('[ERROR] - CREATE_ENGINE')
        sys.exit(2)


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

    log_file = log_path + "/generate_spatial_outputs.log"
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

    # Get dbs config parameters
    config = get_config(config_filepath)

    # Create the log file if not exists
    log_file = create_log_file(config["log_path"])

    # Create the logger
    logger = create_logger(log_file)

    # Connect to 'mapstore' database
    mapstore_connection = create_mapstore_connection(config, logger)

    # Create mapstore's database engine
    mapstore_engine = create_mapstore_engine(mapstore_connection, logger)

    # Open the 'tables_processing.sql' file
    sql_query = open_sql_query(logger)

    # Execute the SQL to preprocces the input tables
    execute_sql_query(mapstore_engine, sql_query, logger)
    
    end = datetime.now()

    print(f"[OK] - Tables successfully copied to mapstore's database. Time elapsed: {end - start}")


if __name__ == "__main__":
    main(sys.argv)