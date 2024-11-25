import pandas as pd
from pandas.io.sql import get_schema
from pymysql.connections import Connection

from . import db


def initialise_database(db_name: str, user: str, password: str, host: str, port: int = 3306) -> \
        Connection | None:
    conn = db.create_connection(user=user, password=password, host=host, port=port, database=None)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    conn = db.create_connection(user=user, password=password, host=host, database=db_name,
                                port=port
                                )
    return conn


def upload_to_database(conn: Connection, table_name: str, df: pd.DataFrame):
    cursor = conn.cursor()

    schema = get_schema(df, table_name)
    create_table_query = schema.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)

    cursor.execute(create_table_query)

    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
    cursor.executemany(insert_query, df.to_records(index=False))

    conn.commit()
