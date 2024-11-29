from datetime import datetime
from typing import Union

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


def create_metadata_table(connection: Connection) -> None:
    if abort_deletion_if_table_exists(connection, "upload_metadata"):
        return

    run_query(connection, """
                                        CREATE TABLE upload_metadata (
                                            pipeline_name VARCHAR(255) PRIMARY KEY,
                                            last_upload_time DATETIME NOT NULL
                                        );
                                    """
              )


def check_previous_pipeline(connection: Connection, pipeline_name: str,
                            expected_minutes: int = None
                            ) -> bool:
    last_upload_info = run_query(connection, f"""
                                                                            SELECT last_upload_time
                                                                            FROM upload_metadata 
                                                                            WHERE pipeline_name = '{pipeline_name}'
                                                                            LIMIT 1;
                                                                        """
                                 )

    if not last_upload_info.empty:
        last_upload_time = last_upload_info.iloc[0]["last_upload_time"]
    else:
        last_upload_time = None

    if last_upload_time:
        print(f"Previous pipeline {pipeline_name} was performed on {last_upload_time}.")

        if expected_minutes:
            print(f"Pipeline is expected to take {str(expected_minutes)} minutes")

        proceed_with_pipeline = input("Do you want to proceed with the pipeline? (y/n): "
                                      ).strip().lower()

        if proceed_with_pipeline in ['y', 'yes']:
            return True
        else:
            print("Aborting pipeline.")
            return False

    else:
        print("No previous evidence of pipeline run. Proceeding...")
        return True


def update_metadata(connection: Connection, pipeline_name: str) -> None:
    curr_time = datetime.utcnow()

    run_query(connection, f"""
                                        INSERT INTO upload_metadata (pipeline_name, last_upload_time)
                                        VALUES ('{pipeline_name}', '{curr_time}')
                                        ON DUPLICATE KEY UPDATE 
                                            last_upload_time = '{curr_time}';
                                    """
              )
