from typing import Union

import pymysql
from pymysql import Connection


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


def abort_deletion_if_table_exists(connection, table_name):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    if table_name in tables:
        delete_confirmation = input(
                f"Table '{table_name}' already exists. Do you want to dellete the table and recreate it? (y/n): "
        ).strip().lower()
        if delete_confirmation in ["y", "yes"]:
            clear_table_query = f"DROP TABLE {table_name}"
            print(f"Deleting table '{table_name}'...")
            cursor.execute(clear_table_query)
        else:
            print("Aborting upload.")
            return True
    return False
