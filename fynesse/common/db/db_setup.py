from datetime import datetime
from typing import Union

import pymysql
from pymysql import Connection

from ..db.db import abort_deletion_if_table_exists, run_query


def initialise_database(db_name: str, user: str, password: str, host: str, port: int = 3306) -> \
        Connection | None:
    conn = create_connection(user=user, password=password, host=host, port=port, database=None)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    conn = create_connection(user=user, password=password, host=host, database=db_name,
                             port=port
                             )
    return conn


def create_connection(user: str, password: str, host: str, database: Union[str, None],
                      port: int = 3306
                      ):
    conn = None
    try:
        if database is None:
            conn = pymysql.connect(user=user,
                                   passwd=password,
                                   host=host,
                                   port=port,
                                   local_infile=1,
                                   )
        else:
            conn = pymysql.connect(user=user,
                                   passwd=password,
                                   host=host,
                                   port=port,
                                   local_infile=1,
                                   db=database
                                   )
        print(f"Connection established!")
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn


def create_metadata_table(conn: Connection):
    if abort_deletion_if_table_exists(conn, "upload_metadata"):
        return

    create_query = """
        CREATE TABLE upload_metadata (
        pipeline_name VARCHAR(255) PRIMARY KEY,
        last_upload_time DATETIME NOT NULL
    );
    """

    run_query(conn, create_query)


def check_previous_upload(connection, pipeline_name):
    check_query = f"""
    SELECT last_upload_time
    FROM upload_metadata 
    WHERE pipeline_name = {pipeline_name}
    LIMIT 1;
    """

    query_result = run_query(connection, check_query)

    last_upload_time = None
    if not query_result.empty:
        last_upload_time = query_result.iloc[0]["last_upload_time"]
    else:
        return None

    if last_upload_time:
        print(f"Previous {pipeline_name} was performed on {last_upload_time}.")
        user_input = input("Do you want to proceed with the pipeline? (y/n): "
                           ).strip().lower()
        if user_input in ['y', 'yes']:
            return True
        else:
            print("Aborting upload.")
            return False
    else:
        print("No previous upload found. Proceeding with the upload.")
        return True


def update_metadata(connection, pipeline_name):
    insert_query = f"""
        INSERT INTO upload_metadata (pipeline_name, last_upload_time)
        VALUES ('{pipeline_name}', '{datetime.utcnow()}')
        ON DUPLICATE KEY UPDATE 
            last_upload_time = '{datetime.utcnow()}';
    """

    run_query(connection, insert_query)
