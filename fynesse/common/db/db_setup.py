from datetime import datetime
from typing import Union, Optional, Tuple

import pandas as pd
import pymysql
from pymysql import Connection

from ..db.db import abort_deletion_if_table_exists, run_query


def initialise_database(db_name: str, user: str, password: str, host: str, port: int = 3306
                        ) -> Connection | None:
    connection_info = {"user": user, "password": password, "host": host, "port": port}

    connection = create_connection(**connection_info, database=None)

    run_query(connection, f"CREATE DATABASE IF NOT EXISTS {db_name}")

    connection = create_connection(**connection_info, database=db_name)

    print(f"Connected to database {db_name}!")
    return connection


def create_connection(user: str, password: str, host: str, database: Union[str, None],
                      port: int = 3306
                      ) -> Connection | None:
    connection_info = {"user": user, "passwd": password, "host": host, "port": port}

    connection = None
    try:
        if database is None:
            connection = pymysql.connect(**connection_info, local_infile=1)
        else:
            connection = pymysql.connect(**connection_info, local_infile=1, db=database)

    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")

    return connection


def initialise_metadata_table(connection: Connection) -> None:
    if abort_deletion_if_table_exists(connection, "pipeline_metadata"):
        return

    run_query(connection, """
                                        CREATE TABLE pipeline_metadata (
                                            pipeline_name VARCHAR(255) PRIMARY KEY,
                                            last_pipeline_start DATETIME NOT NULL  DEFAULT CURRENT_TIMESTAMP,
                                            last_pipeline_end DATETIME
                                        );
                                    """
              )


def create_metadata_table_if_not_exists(connection: Connection) -> None:
    run_query(connection, """
                                        CREATE TABLE IF NOT EXISTS pipeline_metadata (
                                            pipeline_name VARCHAR(255) PRIMARY KEY,
                                            last_pipeline_start DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                            last_pipeline_end DATETIME
                                        );
                                    """
              )


def is_pipeline_in_progress(connection: Connection, pipeline_name: str) -> Tuple[
    bool, Optional[datetime], Optional[datetime]]:
    result = run_query(connection, f"""
                    SELECT last_pipeline_start, last_pipeline_end
                    FROM pipeline_metadata
                    WHERE pipeline_name = '{pipeline_name}';
                """
                       )

    result["last_pipeline_start"] = pd.to_datetime(result["last_pipeline_start"])
    result["last_pipeline_end"] = pd.to_datetime(result["last_pipeline_end"])

    if len(result) > 0 and result["last_pipeline_start"].values[0]:
        last_start = result["last_pipeline_start"].values[0]
        last_end = result["last_pipeline_end"].values[0]
        in_progress = last_end is None or last_start > last_end
        return in_progress, last_start, last_end

    return False, None, None


def update_metadata_on_start_pipeline(connection: Connection, pipeline_name: str) -> None:
    curr_time = datetime.utcnow()

    run_query(connection, f"""
                                        INSERT INTO pipeline_metadata (pipeline_name, last_pipeline_start)
                                        VALUES ('{pipeline_name}', '{curr_time}')
                                        ON DUPLICATE KEY UPDATE 
                                            last_pipeline_start = '{curr_time}';
                                    """
              )


def update_metadata_on_end_pipeline(connection: Connection, pipeline_name: str) -> None:
    curr_time = datetime.utcnow()

    run_query(connection, f"""
                                        INSERT INTO pipeline_metadata (pipeline_name, last_pipeline_end)
                                        VALUES ('{pipeline_name}', '{curr_time}')
                                        ON DUPLICATE KEY UPDATE 
                                            last_pipeline_end = '{curr_time}';
                                    """
              )
