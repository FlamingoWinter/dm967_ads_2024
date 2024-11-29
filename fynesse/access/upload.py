import pandas as pd
from pymysql.connections import Connection

from ..common.db import db_operations
from ..common.db import db_setup


def initialise_database(db_name: str, user: str, password: str, host: str, port: int = 3306) -> \
        Connection | None:
    return db_setup.initialise_database(db_name, user, password, host, port)


def upload_to_database(connection: Connection, table_name: str, df: pd.DataFrame):
    return db_operations.upload_to_database(connection, table_name, df)


def append_to_database(connection: Connection, table_name: str, df: pd.DataFrame):
    return db_operations.append_to_database(connection, table_name, df)
